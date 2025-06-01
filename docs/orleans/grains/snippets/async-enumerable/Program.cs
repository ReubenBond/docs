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
                    .ConfigureLogging(logging => logging.AddConsole());
            })
            .Build();

        await host.StartAsync();

        var client = host.Services.GetRequiredService<IClusterClient>();

        Console.WriteLine("Orleans IAsyncEnumerable Demo");
        Console.WriteLine("=============================");
        Console.WriteLine("This demo progresses from simple to advanced IAsyncEnumerable concepts.\n");        try
        {
            // Use a single grain instance to show progression
            var grain = client.GetGrain<IDataStreamGrain>("learning-demo");

            await Demo1_BasicStreaming(grain);
            await Demo2_StreamingWithCancellation(grain);
            await Demo3_RealtimeStreaming(grain);
            await Demo4_ComplexProcessing(grain);
            await Demo5_BatchingAndPerformance(grain);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }

        await host.StopAsync();
    }    /// <summary>
    /// DEMO 1: Basic streaming with the core GetDataStream method.
    /// Demonstrates the foundation: yield return with proper cancellation support.
    /// </summary>
    private static async Task Demo1_BasicStreaming(IDataStreamGrain grain)
    {
        Console.WriteLine("=== DEMO 1: Basic Streaming ===");
        Console.WriteLine("Shows the core GetDataStream method with cancellation support\n");

        Console.WriteLine("Generating basic data stream...");
        await foreach (var item in grain.GetDataStream(5))
        {
            Console.WriteLine($"  Received: {item}");
        }

        Console.WriteLine("✅ Completed basic streaming\n");
    }

    /// <summary>
    /// DEMO 2: Same method as Demo 1, but with cancellation to show token handling.
    /// Builds on Demo 1 by demonstrating cancellation in action.
    /// </summary>
    private static async Task Demo2_StreamingWithCancellation(IDataStreamGrain grain)
    {
        Console.WriteLine("=== DEMO 2: Streaming with Cancellation ===");
        Console.WriteLine("Uses the same GetDataStream method but with cancellation token\n");

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(800)); // Cancel after 800ms

        try
        {
            Console.WriteLine("Generating data stream (will be canceled after 800ms)...");
            await foreach (var item in grain.GetDataStream(10, 300, cts.Token))
            {
                Console.WriteLine($"  Received: {item}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("  ⚠️  Stream was canceled as expected");
        }

        Console.WriteLine("✅ Completed cancellation demo\n");
    }    /// <summary>
    /// DEMO 3: Real-time streaming using channels with cancellation support.
    /// Builds on previous demos by showing producer-consumer pattern with channels.
    /// </summary>
    private static async Task Demo3_RealtimeStreaming(IDataStreamGrain grain)
    {
        Console.WriteLine("=== DEMO 3: Real-time Streaming ===");
        Console.WriteLine("Shows channel-based streaming with producer-consumer pattern and cancellation\n");

        using var cts = new CancellationTokenSource();

        // Start producing data in the background
        var producer = Task.Run(async () =>
        {
            Console.WriteLine("Producer: Starting to send real-time data...");
            for (int i = 0; i < 6; i++)
            {
                if (cts.Token.IsCancellationRequested) break;

                await grain.WriteToRealtimeStream($"Real-time message {i}");
                Console.WriteLine($"Producer: Sent message {i}");
                await Task.Delay(400, cts.Token);
            }
            await grain.CompleteRealtimeStream();
            Console.WriteLine("Producer: Stream completed");
        });

        // Consume the real-time stream with cancellation support
        Console.WriteLine("Consumer: Starting to receive data...");
        try
        {
            await foreach (var item in grain.GetRealtimeStream(cts.Token))
            {
                Console.WriteLine($"Consumer: Received '{item}'");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consumer: Stream consumption was canceled");
        }

        await producer; // Ensure producer completes
        Console.WriteLine("✅ Completed real-time streaming\n");
    }    /// <summary>
    /// DEMO 4: Complex object streaming with processing simulation.
    /// Builds on all previous concepts: cancellation, complex objects, and variable processing times.
    /// </summary>
    private static async Task Demo4_ComplexProcessing(IDataStreamGrain grain)
    {
        Console.WriteLine("=== DEMO 4: Complex Processing ===");
        Console.WriteLine("Shows streaming of complex objects with variable processing times and cancellation\n");

        Console.WriteLine("Processing complex data stream...");
        await foreach (var result in grain.GetProcessedDataStream(4))
        {
            Console.WriteLine($"  {result.Data} at {result.ProcessedAt:HH:mm:ss.fff}");
        }

        Console.WriteLine("✅ Completed complex processing\n");
    }    /// <summary>
    /// DEMO 5: Performance optimization with batching using the core GetDataStream method.
    /// Shows how WithBatchSize() affects delivery of the same underlying stream.
    /// </summary>
    private static async Task Demo5_BatchingAndPerformance(IDataStreamGrain grain)
    {
        Console.WriteLine("=== DEMO 5: Batching and Performance ===");
        Console.WriteLine("Shows WithBatchSize() for performance optimization using the same core method\n");

        // Demonstrate different batch sizes using the same GetDataStream method
        Console.WriteLine("Testing different batch sizes with the same GetDataStream method...");

        Console.WriteLine("\nUsing default batching:");
        var count = 0;
        await foreach (var item in grain.GetDataStream(8, 50)) // Faster for demo
        {
            Console.WriteLine($"  [{++count}] {item}");
        }

        Console.WriteLine("\nUsing batch size of 1 (immediate delivery):");
        count = 0;
        await foreach (var item in grain.GetDataStream(8, 50).WithBatchSize(1))
        {
            Console.WriteLine($"  [{++count}] {item}");
        }

        Console.WriteLine("✅ Completed batching demonstration\n");

        Console.WriteLine("🎉 All demos completed successfully!");
        Console.WriteLine("You've learned: Basic streaming → Cancellation → Real-time → Complex processing → Performance");
        Console.WriteLine("Key insight: All scenarios use the same foundational patterns with consistent CancellationToken support!");
    }
}
