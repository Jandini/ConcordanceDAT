using Concordance.Dat;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;

internal class Main(ILogger<Main> logger)
{
    public async Task<int> RunAsync(CancellationToken cancellationToken)
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
            MaxDegreeOfParallelism = 8,
            CancellationToken = cancellationToken
        };

        await Parallel.ForEachAsync(datFiles, parallel, async (datFile, ct) =>
        {
            logger.LogInformation("Reading {file}", datFile);

            var sw = Stopwatch.StartNew();
            var lastProgressUpdate = Stopwatch.StartNew();
            int rowNumber = 0;

            try
            {
                await foreach (var _ in DatFile1.ReadAsync(datFile, ct))
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

                // Capture memory snapshots (working set = process resident; managed = GC heap)
                var workingSetMB = Process.GetCurrentProcess().WorkingSet64 / (1024.0 * 1024.0);
                var managedMB = GC.GetTotalMemory(forceFullCollection: false) / (1024.0 * 1024.0);

                logger.LogInformation(
                    "Read {count} rows in {elapsed} ({rps:F2} rows/sec) | Working Set: {ws:N2} MB | Managed: {managed:N2} MB",
                    rowNumber, sw.Elapsed, rowsPerSecond, workingSetMB, managedMB
                );

                lock (aggLock)
                {
                    totalRows += rowNumber;
                    totalMemoryMB += workingSetMB;   // average-of-snapshots; for max, track separately if needed
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

        logger.LogInformation(
            "Total {count} rows completed in {elapsed} for {files} files ({failed} failed) | Avg WS: {avgMem:N2} MB | Avg Rows/sec: {avgRps:F2}",
            totalRows, totalWatch.Elapsed, fileCount, errors.Count, avgMemoryMB, avgRowsPerSecond
        );

        foreach (var error in errors)
            logger.LogError("{file:l}: {error:l}", error.Key, error.Value);

        return 0;
    }
}
