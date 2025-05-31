using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace GrainCallStreaming;

/// <summary>
/// Grain implementation that demonstrates IAsyncEnumerable streaming using channels.
/// </summary>
public class StreamingGrain : Grain, IStreamingGrain
{
    private readonly Channel<string> _dataChannel = Channel.CreateUnbounded<string>();

    public Task AddData(string data)
    {
        if (!_dataChannel.Writer.TryWrite(data))
        {
            throw new InvalidOperationException("Channel is closed");
        }
        return Task.CompletedTask;
    }

    public ValueTask Complete()
    {
        _dataChannel.Writer.Complete();
        return ValueTask.CompletedTask;
    }

    public IAsyncEnumerable<string> GetDataStream()
    {
        return _dataChannel.Reader.ReadAllAsync();
    }

    public IAsyncEnumerable<string> GetDataStreamWithCancellation(CancellationToken cancellationToken = default)
    {
        return _dataChannel.Reader.ReadAllAsync(cancellationToken);
    }
}

/// <summary>
/// Grain implementation that generates numbers using async generator methods.
/// </summary>
public class NumberGeneratorGrain : Grain, INumberGeneratorGrain
{
    public async IAsyncEnumerable<int> GenerateNumbers(
        int count,
        int delayMs,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await Task.Delay(delayMs, cancellationToken);
            yield return i;
        }
    }

    public async IAsyncEnumerable<long> GenerateFibonacci(
        int count,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        long a = 0, b = 1;

        for (int i = 0; i < count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            yield return a;

            (a, b) = (b, a + b);

            // Add a small delay to make the streaming effect visible
            await Task.Delay(50, cancellationToken);
        }
    }
}

/// <summary>
/// Grain implementation that demonstrates batch processing scenarios.
/// </summary>
public class BatchProcessorGrain : Grain, IBatchProcessorGrain
{
    public async IAsyncEnumerable<ProcessingResult> ProcessLargeDataset(
        int itemCount,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < itemCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Simulate processing work
            await Task.Delay(Random.Shared.Next(50, 200), cancellationToken);

            yield return new ProcessingResult(
                Id: i,
                Data: $"Processed item {i}",
                ProcessedAt: DateTime.UtcNow
            );
        }
    }
}
