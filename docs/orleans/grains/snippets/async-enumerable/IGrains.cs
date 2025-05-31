namespace GrainCallStreaming;

/// <summary>
/// Interface for a grain that demonstrates IAsyncEnumerable streaming capabilities.
/// </summary>
public interface IStreamingGrain : IGrainWithStringKey
{
    /// <summary>
    /// Adds data to the stream.
    /// </summary>
    Task AddData(string data);

    /// <summary>
    /// Signals that no more data will be added to the stream.
    /// </summary>
    ValueTask Complete();

    /// <summary>
    /// Gets all data as an async enumerable stream.
    /// </summary>
    IAsyncEnumerable<string> GetDataStream();

    /// <summary>
    /// Gets data stream with cancellation support.
    /// </summary>
    IAsyncEnumerable<string> GetDataStreamWithCancellation(CancellationToken cancellationToken = default);
}

/// <summary>
/// Interface for a grain that generates numbers using async enumerable.
/// </summary>
public interface INumberGeneratorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Generates a sequence of numbers with specified delay between each number.
    /// </summary>
    IAsyncEnumerable<int> GenerateNumbers(int count, int delayMs, CancellationToken cancellationToken = default);

    /// <summary>
    /// Generates fibonacci numbers up to a specified count.
    /// </summary>
    IAsyncEnumerable<long> GenerateFibonacci(int count, CancellationToken cancellationToken = default);
}

/// <summary>
/// Interface for a grain that demonstrates batch processing scenarios.
/// </summary>
public interface IBatchProcessorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Processes a large dataset and returns results as they become available.
    /// </summary>
    IAsyncEnumerable<ProcessingResult> ProcessLargeDataset(int itemCount, CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents the result of a processing operation.
/// </summary>
[GenerateSerializer]
public record ProcessingResult(int Id, string Data, DateTime ProcessedAt);
