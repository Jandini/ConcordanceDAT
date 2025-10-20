using Concordance.Dat;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

internal class Main(ILogger<Main> logger, SplitDat splitter)
{
    private readonly SplitDat _splitter = splitter;

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        // Discover input files
        var datFiles = Directory.GetFiles(@"c:\dat", "*.dat");

        // Aggregates (thread-safe via lock)
        long totalRows = 0;
        double totalMemoryMB = 0;
        double totalSeconds = 0;
        int fileCount = 0;
        var aggLock = new object();

        var totalWatch = Stopwatch.StartNew();
        var errors = new ConcurrentDictionary<string, string>();

        // Tune parallelism for your storage. Start modestly; measure and adjust.
        var parallel = new ParallelOptions
        {
            MaxDegreeOfParallelism = 1,
            CancellationToken = cancellationToken
        };

        // Example: split the first file into multiple files with up to 10,000 rows each using DI service
        if (datFiles.Length > 0)
        {
            var first = datFiles[0];
            var outDir = Path.Combine(Path.GetDirectoryName(first) ?? ".", "splits");
            logger.LogInformation("Splitting {file} into chunks of {max} rows in {out}", first, 10000, outDir);
            try
            {
                await _splitter.RunAsync(first, outDir, 10_000, cancellationToken: cancellationToken);
                logger.LogInformation("Split complete for {file}", first);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to split {file}", first);
            }
        }

        await Parallel.ForEachAsync(datFiles, parallel, async (datFile, ct) =>
        {

            logger.LogInformation("Reading {file}", datFile);

            var header = await DatFile.GetHeaderAsync(datFile, ct);

            logger.LogInformation("Found {count} fields in the header.", header.Count);

            var sw = Stopwatch.StartNew();
            var lastProgressUpdate = Stopwatch.StartNew();
            int rowNumber = 0;

            try
            {
                await foreach (var _ in DatFile.ReadAsync(datFile, ct))
                {
                    rowNumber++;

                    // Throttle UI updates to avoid contention (every ~1s)
                    if (lastProgressUpdate.ElapsedMilliseconds >= 1000)
                    {
                        Console.Title = $"{Path.GetFileName(datFile)} : {rowNumber:N0}";
                        lastProgressUpdate.Restart();
                    }
                }

                var seconds = sw.Elapsed.TotalSeconds;
                var rowsPerSecond = seconds > 0 ? rowNumber / seconds : 0;


                logger.LogInformation("Read {count} rows in {elapsed} ({rps:F2} rows/sec)", rowNumber, sw.Elapsed, rowsPerSecond);

                lock (aggLock)
                {
                    totalRows += rowNumber;
                    totalSeconds += seconds;
                    fileCount++;
                }
            }
            catch (Exception ex)
            {
                errors[datFile] = ex.Message;
                logger.LogError(ex, "Error in {file} at row {row}", datFile, rowNumber);
            }
        });

        totalWatch.Stop();

        var avgMemoryMB = fileCount > 0 ? totalMemoryMB / fileCount : 0;
        var avgRowsPerSecond = totalSeconds > 0 ? totalRows / totalSeconds : 0;


        foreach (var error in errors)
            logger.LogError("{file:l}: {error:l}", error.Key, error.Value);

        logger.LogInformation(
            "Total {count} rows completed in {elapsed} for {files} files ({failed} failed) | Avg WS: {avgMem:N2} MB | Avg Rows/sec: {avgRps:F2}",
            totalRows, totalWatch.Elapsed, fileCount, errors.Count, avgMemoryMB, avgRowsPerSecond
        );

    }
}
