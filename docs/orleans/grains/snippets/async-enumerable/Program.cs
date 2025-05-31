using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace GrainCallStreaming;

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder(args)
            .UseOrleans(siloBuilder =>
            {
                siloBuilder
                    .UseLocalhostClustering()
                    .ConfigureLogging(logging => logging.AddConsole());            })
            .Build();

        await host.StartAsync();

        var client = host.Services.GetRequiredService<IClusterClient>();

        Console.WriteLine("Orleans IAsyncEnumerable Demo");
        Console.WriteLine("=============================\n");

        try
        {
            await DemoBasicStreaming(client);
            await DemoNumberGeneration(client);
            await DemoBatchProcessing(client);
            await DemoCancellation(client);
            await DemoBatching(client);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }

        await host.StopAsync();
    }

    /// <summary>
    /// Demonstrates basic streaming using channels.
    /// </summary>
    private static async Task DemoBasicStreaming(IClusterClient client)
    {
        Console.WriteLine("1. Basic Streaming Demo");
        Console.WriteLine("-----------------------");

        var grain = client.GetGrain<IStreamingGrain>("demo1");

        // Start a background task to add data
        var producer = Task.Run(async () =>
        {
            for (int i = 0; i < 5; i++)
            {
                await grain.AddData($"Message {i}");
                await Task.Delay(500);
            }
            await grain.Complete();
        });

        // Consume the stream
        await foreach (var item in grain.GetDataStream())
        {
            Console.WriteLine($"  Received: {item}");
        }

        Console.WriteLine("  Stream completed.\n");
    }

    /// <summary>
    /// Demonstrates number generation using async generators.
    /// </summary>
    private static async Task DemoNumberGeneration(IClusterClient client)
    {
        Console.WriteLine("2. Number Generation Demo");
        Console.WriteLine("-------------------------");

        var grain = client.GetGrain<INumberGeneratorGrain>("generator1");

        Console.WriteLine("  Generating numbers:");
        await foreach (var number in grain.GenerateNumbers(5, 300))
        {
            Console.WriteLine($"    Generated: {number}");
        }

        Console.WriteLine("\n  Generating Fibonacci sequence:");
        await foreach (var fib in grain.GenerateFibonacci(8))
        {
            Console.WriteLine($"    Fibonacci: {fib}");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates batch processing scenarios.
    /// </summary>
    private static async Task DemoBatchProcessing(IClusterClient client)
    {
        Console.WriteLine("3. Batch Processing Demo");
        Console.WriteLine("------------------------");

        var grain = client.GetGrain<IBatchProcessorGrain>("processor1");

        Console.WriteLine("  Processing dataset:");
        await foreach (var result in grain.ProcessLargeDataset(5))
        {
            Console.WriteLine($"    {result.Data} at {result.ProcessedAt:HH:mm:ss.fff}");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates cancellation support.
    /// </summary>
    private static async Task DemoCancellation(IClusterClient client)
    {
        Console.WriteLine("4. Cancellation Demo");
        Console.WriteLine("--------------------");

        var grain = client.GetGrain<INumberGeneratorGrain>("generator2");

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(2)); // Cancel after 2 seconds

        try
        {
            Console.WriteLine("  Generating numbers (will be canceled):");
            await foreach (var number in grain.GenerateNumbers(10, 500).WithCancellation(cts.Token))
            {
                Console.WriteLine($"    Generated: {number}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("    Stream was canceled as expected.");
        }

        Console.WriteLine();
    }

    /// <summary>
    /// Demonstrates different batch sizes.
    /// </summary>
    private static async Task DemoBatching(IClusterClient client)
    {
        Console.WriteLine("5. Batching Demo");
        Console.WriteLine("----------------");

        var grain = client.GetGrain<IStreamingGrain>("demo2");

        // Add multiple items quickly
        for (int i = 0; i < 10; i++)
        {
            await grain.AddData($"Batch item {i}");
        }
        await grain.Complete();

        Console.WriteLine("  Consuming with default batching:");
        var count = 0;
        await foreach (var item in grain.GetDataStream())
        {
            Console.WriteLine($"    [{++count}] {item}");
        }

        // Reset for next demo
        var grain2 = client.GetGrain<IStreamingGrain>("demo3");
        for (int i = 0; i < 10; i++)
        {
            await grain2.AddData($"Single item {i}");
        }
        await grain2.Complete();

        Console.WriteLine("\n  Consuming with batch size 1 (no batching):");
        count = 0;
        await foreach (var item in grain2.GetDataStream().WithBatchSize(1))
        {
            Console.WriteLine($"    [{++count}] {item}");
        }

        Console.WriteLine();
    }
}
