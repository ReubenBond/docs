namespace GrainCallStreaming;

/// <summary>
/// A single grain interface that demonstrates all IAsyncEnumerable concepts,
/// building from simple to advanced scenarios.
/// </summary>
public interface IDataStreamGrain : IGrainWithStringKey
{    // === BASIC STREAMING (builds foundation) ===

    /// <summary>
    /// Basic async enumerable that generates simple data with cancellation support.
    /// Demonstrates the foundation of IAsyncEnumerable in Orleans.
    /// </summary>
    IAsyncEnumerable<string> GetDataStream(int count, int delayMs = 100, CancellationToken cancellationToken = default);

    // === REAL-TIME STREAMING (builds on basic streaming) ===

    /// <summary>
    /// Gets a real-time data stream that can be written to by producers.
    /// Demonstrates channel-based streaming with cancellation support.
    /// </summary>
    IAsyncEnumerable<string> GetRealtimeStream(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds data to the real-time stream for immediate consumption.
    /// </summary>
    ValueTask WriteToRealtimeStream(string data);

    /// <summary>
    /// Signals completion of the real-time stream.
    /// </summary>
    ValueTask CompleteRealtimeStream();    // === COMPLEX DATA PROCESSING (builds on all previous concepts) ===

    /// <summary>
    /// Demonstrates complex object streaming with processing simulation.
    /// Combines all concepts: cancellation, complex objects, and variable processing times.
    /// </summary>
    IAsyncEnumerable<ProcessingResult> GetProcessedDataStream(int itemCount, int minDelayMs = 50, int maxDelayMs = 300, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents a processing result with metadata.
/// </summary>
[GenerateSerializer]
public record ProcessingResult(int Id, string Data, DateTime ProcessedAt);
