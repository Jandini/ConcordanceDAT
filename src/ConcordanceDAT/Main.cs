using ConcordanceDAT;
using Microsoft.Extensions.Logging;
using System.Diagnostics;

internal class Main(ILogger<Main> logger)
{

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {      
        var datFiles = Directory.GetFiles(@"C:\Temp\load\dat", "*.dat");
        var totalRows = 0;
        var totalWatch = new Stopwatch();

        totalWatch.Start();

        foreach (var datFile in datFiles)
        {
            logger.LogInformation(datFile);
            var stopwatch = new Stopwatch();

            stopwatch.Start();

            int rowNumber = 0;

            using (var stream = new FileStream(datFile, FileMode.Open, FileAccess.Read, FileShare.None, 1024 * 1024))
            {
                var rows = DATFile.ReadAsync(stream, cancel: cancellationToken);

                await foreach (var row in rows)
                {
                    rowNumber++;

                    if (rowNumber % 8192 == 0)
                        Console.Title = rowNumber.ToString();
                }
            }

            totalRows += rowNumber;

            logger.LogInformation("Read {count} rows completed in {elapsed}", rowNumber, stopwatch.Elapsed);
        }

        totalWatch.Stop();
        logger.LogInformation("Total {count} rows completed in {elapsed}", totalRows, totalWatch.Elapsed);

        return 0;
    }
}
