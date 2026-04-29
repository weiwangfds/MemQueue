// src/MemQueue/Internal/SubscriptionManager.cs
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Internal;

internal sealed class SubscriptionManager : IAsyncDisposable
{
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(10);

    private readonly List<Task> _tasks = new();
    private readonly List<IAsyncDisposable> _subscriptions = new();
    private readonly object _lock = new();

    // B7 fix: add TCS placeholder to _tasks BEFORE Task.Run, so DisposeAsync can't miss it
    public Task StartAsync<TMessage>(
        IConsumer<TMessage> consumer,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        bool autoCommit,
        CancellationToken cancellationToken,
        Action<Exception, TopicId, PartitionId, Offset>? onError = null,
        Action? onCleanup = null,
        IDeadLetterQueue? deadLetterQueue = null) where TMessage : MessageBase
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        lock (_lock)
        {
            _subscriptions.Add(consumer);
            _tasks.Add(tcs.Task); // placeholder registered before Task.Run
        }

        Task.Run(async () =>
        {
            try
            {
                await foreach (var result in consumer.ConsumeAllAsync(cts.Token))
                {
                    var context = new MessageContext
                    {
                        Topic = result.Topic,
                        Partition = result.Partition,
                        Offset = result.Offset,
                        Key = result.Key,
                        Timestamp = result.Timestamp,
                        CommitFunc = (off, part, ct2) => consumer.CommitAsync(off, part, ct2)
                    };

                    try
                    {
                        await handler(result.Value, context, cts.Token);
                        if (autoCommit) await context.CommitAsync(cts.Token);
                    }
                    catch (OperationCanceledException) when (cts.Token.IsCancellationRequested) { }
                    catch (Exception ex)
                    {
                        onError?.Invoke(ex, result.Topic, result.Partition, result.Offset);
                        deadLetterQueue?.Publish(new DeliveryError
                        {
                            Topic = result.Topic.Value,
                            Partition = result.Partition.Value,
                            Offset = result.Offset.Value,
                            ErrorType = ex.GetType().Name,
                            ErrorMessage = ex.Message
                        }, result.Value);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                tcs.SetResult(); // signal completion
                cts.Dispose();
                onCleanup?.Invoke();
            }
        }, cancellationToken);

        return Task.CompletedTask;
    }

    public async Task ShutdownAsync(TimeSpan timeout)
    {
        List<IAsyncDisposable> subs;
        List<Task> tasks;
        lock (_lock) { subs = [.._subscriptions]; tasks = [.._tasks]; }

        foreach (var s in subs) await s.DisposeAsync();
        if (tasks.Count > 0)
            await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(timeout));
    }

    public async ValueTask DisposeAsync()
    {
        List<IAsyncDisposable> subs;
        List<Task> tasks;
        lock (_lock) { subs = [.._subscriptions]; tasks = [.._tasks]; }

        foreach (var s in subs) await s.DisposeAsync();
        if (tasks.Count > 0)
            await Task.WhenAny(Task.WhenAll(tasks), Task.Delay(ShutdownTimeout));
    }
}
