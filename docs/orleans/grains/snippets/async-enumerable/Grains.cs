using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace GrainCallStreaming;

/// <summary>
/// A single grain implementation that demonstrates all IAsyncEnumerable concepts,
/// building from simple to advanced scenarios with consistent CancellationToken usage.
/// </summary>
public class DataStreamGrain : Grain, IDataStreamGrain
{
    private readonly Channel<string> _realtimeChannel = Channel.CreateUnbounded<string>();

    // === CORE STREAMING METHOD (foundation for all scenarios) ===

    /// <summary>
    /// Core streaming method that demonstrates proper cancellation token handling.
    /// All other streaming methods build upon this foundation.
    /// </summary>
    public async IAsyncEnumerable<string> GetDataStream(
        int count,
        int delayMs = 100,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < count; i++)
        {
            // Check for cancellation before each iteration
            cancellationToken.ThrowIfCancellationRequested();

            // Pass cancellation token to async operations
            await Task.Delay(delayMs, cancellationToken);
            yield return $"Item {i}";
        }
    }

    // === REAL-TIME STREAMING (builds on core streaming with channels) ===

    /// <summary>
    /// Gets a real-time data stream with cancellation support.
    /// Demonstrates channel-based streaming that respects cancellation tokens.
    /// </summary>
    public async IAsyncEnumerable<string> GetRealtimeStream(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in _realtimeChannel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    /// Writes data to the real-time stream for immediate consumption.
    /// </summary>
    public ValueTask WriteToRealtimeStream(string data)
    {
        if (!_realtimeChannel.Writer.TryWrite(data))
        {
            throw new InvalidOperationException("Real-time stream has been completed");
        }
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Signals completion of the real-time stream.
    /// </summary>
    public ValueTask CompleteRealtimeStream()
    {
        _realtimeChannel.Writer.Complete();
        return ValueTask.CompletedTask;
    }

    // === COMPLEX DATA PROCESSING (builds on all previous concepts) ===

    /// <summary>
    /// Demonstrates complex object streaming with processing simulation.
    /// Combines cancellation, complex objects, and variable processing times.
    /// </summary>
    public async IAsyncEnumerable<ProcessingResult> GetProcessedDataStream(
        int itemCount,
        int minDelayMs = 50,
        int maxDelayMs = 300,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < itemCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Simulate variable processing time within specified range
            var processingTime = Random.Shared.Next(minDelayMs, maxDelayMs + 1);
            await Task.Delay(processingTime, cancellationToken);

            yield return new ProcessingResult(
                Id: i,
                Data: $"Processed item {i} (took {processingTime}ms)",
                ProcessedAt: DateTime.UtcNow
            );
        }
    }
}
