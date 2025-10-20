// Created with JandaBox http://github.com/Jandini/JandaBox
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

try
{
    using var serviceProvider = new ServiceCollection()
        .AddLogging(builder => builder.AddSerilog(new LoggerConfiguration()
            .Enrich.WithStopwatch(string.Empty)
            .Enrich.WithIoMetrics()
            .Enrich.WithMemoryMetrics()
            .WriteTo.Console(
                theme: AnsiConsoleTheme.Code,
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} ({Stopwatch}) [{Level:u4}] {Message} | Read {IoReadBytes:N0} | Memory {WorkingSetBytes:N0}{NewLine}{Exception}")
            .WriteTo.File($"logs/concordance-{DateTime.Now.ToString("yyyMMdd-HHmmss")}.log",
                outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss} [{Level:u4}] {Message} | Elapsed {Stopwatch} | Read {IoReadBytes:N0} | Memory {WorkingSetBytes:N0}{NewLine}{Exception}")
            .CreateLogger(), dispose: true))
        .AddTransient<Main>()
        .AddTransient<SplitDat>()
        .BuildServiceProvider();

    try
    {
        using var cancellationTokenSource = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            if (!cancellationTokenSource.IsCancellationRequested)
            {
                serviceProvider.GetRequiredService<ILogger<Program>>()
                    .LogWarning("User break (Ctrl+C) detected. Shutting down gracefully...");

                cancellationTokenSource.Cancel();
                eventArgs.Cancel = true;
            }
        };

        await serviceProvider
            .GetRequiredService<Main>()
            .RunAsync(cancellationTokenSource.Token);
    }
    catch (Exception ex)
    {
        serviceProvider.GetService<ILogger<Program>>()
            .LogCritical(ex, "Program failed.");
    }
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
}
