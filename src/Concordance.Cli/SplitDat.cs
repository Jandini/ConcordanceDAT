using Concordance.Dat;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Runtime.CompilerServices;

internal sealed class SplitDat(ILogger<SplitDat> logger)
{

    /// <summary>
    /// Splits the input DAT into multiple files containing up to maxRowsPerFile rows each.
    /// This implementation streams directly from reader to writer and buffers only a single row.
    /// </summary>
    public async Task<long> RunAsync(string inputPath, string outputDirectory, int maxRowsPerFile = 10_000, DatFileOptions options = null, Encoding encoding = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(inputPath);
        Directory.CreateDirectory(outputDirectory);

        long fileIndex = 0;

        await using var src = DatFile.ReadAsync(inputPath, options, cancellationToken).GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            // Advance to the next row; if none, we're done.
            if (!await src.MoveNextAsync().ConfigureAwait(false))
                break;

            fileIndex++;
            var outPath = Path.Combine(outputDirectory, $"{Path.GetFileNameWithoutExtension(inputPath)}-{fileIndex:0000}.dat");
            logger.LogInformation("Creating split file {file} starting at next row", outPath);

            // Local iterator that yields up to maxRowsPerFile rows from the shared enumerator.
            async IAsyncEnumerable<Dictionary<string, object>> Batch([EnumeratorCancellation] CancellationToken ct = default)
            {
                // The enumerator has already been advanced once for this batch; yield the current item first.
                ct.ThrowIfCancellationRequested();
                yield return src.Current;

                var written = 1;
                while (written < maxRowsPerFile && await src.MoveNextAsync().ConfigureAwait(false))
                {
                    ct.ThrowIfCancellationRequested();
                    yield return src.Current;
                    written++;
                }
            }

            // Write this batch to a file. DatFile.WriteAsync will consume the Batch iterator.
            var writtenCount = await DatFile.WriteAsync(outPath, Batch(cancellationToken), options, encoding, cancellationToken: cancellationToken).ConfigureAwait(false);
            logger.LogInformation("Wrote {count} rows to {file}", writtenCount, outPath);

            // If writtenCount < maxRowsPerFile, the source has been exhausted and outer loop will exit on next MoveNextAsync.
        }

        return fileIndex;
    }
}
