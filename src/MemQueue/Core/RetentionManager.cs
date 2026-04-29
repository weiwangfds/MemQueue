// src/MemQueue/Core/RetentionManager.cs
using MemQueue.Abstractions;

namespace MemQueue.Core;

public sealed class RetentionManager : IAsyncDisposable
{
    private readonly ITopicManager _topicManager;
    private readonly TimeSpan _interval;
    private readonly CancellationTokenSource _cts = new();
    private Task? _loopTask;
    private bool _disposed;

    public RetentionManager(ITopicManager topicManager, TimeSpan? interval = null)
    {
        _topicManager = topicManager;
        _interval = interval ?? TimeSpan.FromSeconds(30);
    }

    public Task StartAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _loopTask = RunLoopAsync(ct);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        if (_disposed) return;
        await _cts.CancelAsync();
        if (_loopTask is not null)
        {
            var completed = await Task.WhenAny(_loopTask, Task.Delay(Timeout.Infinite, ct));
            if (completed != _loopTask)
                return; // caller's CT triggered — don't hang
        }
    }

    private async Task RunLoopAsync(CancellationToken externalCt)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token, externalCt);
        using var timer = new PeriodicTimer(_interval);
        try
        {
            while (await timer.WaitForNextTickAsync(linkedCts.Token))
            {
                foreach (var topic in _topicManager.GetTopics())
                {
                    var options = _topicManager.GetTopicOptions(topic);
                    if (options.Retention is null) continue;
                    foreach (var store in _topicManager.GetAllPartitionStores(topic))
                        store.ApplyRetention(options.Retention);
                }
            }
        }
        catch (OperationCanceledException) { }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _cts.CancelAsync();
        if (_loopTask is not null) await _loopTask;
        _cts.Dispose();
    }
}
