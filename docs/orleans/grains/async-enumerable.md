---
title: Stream data with IAsyncEnumerable in Orleans grains
description: Learn how to use IAsyncEnumerable in Orleans grain methods for streaming data and asynchronous enumeration.
ms.date: 05/31/2025
---

# Stream data with IAsyncEnumerable in Orleans grains

Orleans supports streaming data from grain methods using <xref:System.Collections.Generic.IAsyncEnumerable%601>. This feature enables grains to return streams of data that can be consumed asynchronously, making it ideal for scenarios like real-time data feeds, batch processing results, or streaming large datasets.

## Overview

When a grain method returns `IAsyncEnumerable<T>`, Orleans automatically generates a proxy that handles the streaming mechanics. The consumer can use `await foreach` to iterate through the stream, and Orleans manages the underlying communication between the client and the grain.

Key benefits of using `IAsyncEnumerable<T>` in Orleans:

- **Efficient streaming**: Data is streamed on-demand rather than loading everything into memory at once
- **Automatic batching**: Orleans automatically batches multiple items to reduce network round trips  
- **Cancellation support**: Full support for <xref:System.Threading.CancellationToken> to stop enumeration
- **Long polling**: Handles slow producers with automatic heartbeat mechanism
- **Resource management**: Automatic cleanup of resources when enumeration completes or is canceled

## Basic usage

### Define the grain interface

Add an `IAsyncEnumerable<T>` method to your grain interface:

```csharp
public interface IDataStreamGrain : IGrainWithGuidKey
{
    Task AddData(string data);
    ValueTask Complete();
    IAsyncEnumerable<string> GetDataStream();
}
```

### Implement the grain

Use any mechanism that returns `IAsyncEnumerable<T>`, such as channels, async generators, or other async enumerable sources:

```csharp
using System.Threading.Channels;

public class DataStreamGrain : Grain, IDataStreamGrain
{
    private readonly Channel<string> _dataChannel = Channel.CreateUnbounded<string>();

    public Task AddData(string data)
    {
        _dataChannel.Writer.TryWrite(data);
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
}
```

### Consume the stream

Use `await foreach` to consume the async enumerable:

```csharp
var grain = grainFactory.GetGrain<IDataStreamGrain>(Guid.NewGuid());

// Start a background task to add data
var producer = Task.Run(async () =>
{
    for (int i = 0; i < 10; i++)
    {
        await grain.AddData($"Item {i}");
        await Task.Delay(100);
    }
    await grain.Complete();
});

// Consume the stream
await foreach (var item in grain.GetDataStream())
{
    Console.WriteLine($"Received: {item}");
}
```

## Using async generators

You can also implement streaming using async generator methods with `yield return`:

```csharp
public interface INumberStreamGrain : IGrainWithGuidKey
{
    IAsyncEnumerable<int> GenerateNumbers(int count, int delayMs);
}

public class NumberStreamGrain : Grain, INumberStreamGrain
{
    public async IAsyncEnumerable<int> GenerateNumbers(int count, int delayMs)
    {
        for (int i = 0; i < count; i++)
        {
            await Task.Delay(delayMs);
            yield return i;
        }
    }
}
```

## Cancellation support

Orleans supports cancellation tokens with `IAsyncEnumerable<T>` methods. Add a `CancellationToken` parameter to your grain interface method:

```csharp
public interface IDataStreamGrain : IGrainWithGuidKey
{
    IAsyncEnumerable<string> GetDataStream(CancellationToken cancellationToken = default);
}
```

Implement cancellation in your grain:

```csharp
public class DataStreamGrain : Grain, IDataStreamGrain
{
    public async IAsyncEnumerable<string> GetDataStream(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < 1000; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            
            await Task.Delay(100, cancellationToken);
            yield return $"Item {i}";
        }
    }
}
```

### Consuming with cancellation

Pass a cancellation token when consuming the stream:

```csharp
var grain = grainFactory.GetGrain<IDataStreamGrain>(Guid.NewGuid());

using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(5)); // Cancel after 5 seconds

try
{
    await foreach (var item in grain.GetDataStream(cts.Token))
    {
        Console.WriteLine($"Received: {item}");
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Stream was canceled");
}
```

You can also use the `WithCancellation` extension method:

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(5));

await foreach (var item in grain.GetDataStream().WithCancellation(cts.Token))
{
    Console.WriteLine($"Received: {item}");
}
```

## Performance and batching

Orleans automatically batches items to improve performance. You can control the batch size using the `WithBatchSize` extension method:

```csharp
// Use larger batches for better throughput
await foreach (var item in grain.GetDataStream().WithBatchSize(50))
{
    Console.WriteLine($"Received: {item}");
}

// Use smaller batches for lower latency
await foreach (var item in grain.GetDataStream().WithBatchSize(1))
{
    Console.WriteLine($"Received: {item}");
}
```

## Error handling

Handle exceptions that occur during enumeration:

```csharp
try
{
    await foreach (var item in grain.GetDataStream())
    {
        Console.WriteLine($"Received: {item}");
    }
}
catch (EnumerationAbortedException ex)
{
    Console.WriteLine($"Enumeration was aborted: {ex.Message}");
}
catch (Exception ex)
{
    Console.WriteLine($"Error during enumeration: {ex.Message}");
}
```

The <xref:Orleans.Runtime.EnumerationAbortedException> is thrown when the grain is deactivated or the enumeration is otherwise terminated unexpectedly.

## How it works

When you call a grain method that returns `IAsyncEnumerable<T>`, Orleans:

1. **Creates a proxy**: Returns an <xref:Orleans.Runtime.AsyncEnumerableRequest%601> proxy object
2. **Generates request ID**: Creates a unique GUID to track the enumeration
3. **Invokes the method**: Calls the grain method and stores the result
4. **Streams data**: Uses `MoveNext` calls to retrieve data incrementally
5. **Batches items**: Automatically batches multiple items to reduce network calls
6. **Handles timeouts**: Uses heartbeats for slow producers (default 10 seconds)
7. **Manages cleanup**: Automatically disposes resources when enumeration completes

## Best practices

- **Use for streaming scenarios**: Best suited for data that arrives over time or large datasets
- **Consider batch sizes**: Larger batches improve throughput, smaller batches reduce latency
- **Handle cancellation**: Always support cancellation for responsive applications
- **Implement proper disposal**: Ensure your enumerable sources are properly disposed
- **Monitor performance**: Be aware of memory usage and network traffic patterns

## Comparison with Orleans Streams

| Feature | IAsyncEnumerable<T> | Orleans Streams |
|---------|-------------------|-----------------|
| **Setup complexity** | Simple | More complex |
| **Use case** | Direct grain-to-consumer | Producer-consumer patterns |
| **Multiple consumers** | Single consumer | Multiple consumers |
| **Persistence** | No | Optional |
| **Backpressure** | Built-in | Manual handling |
| **Resource usage** | Lower overhead | Higher overhead |

Use `IAsyncEnumerable<T>` for direct streaming from grains to consumers. Use Orleans Streams for more complex pub-sub scenarios with multiple producers and consumers.
