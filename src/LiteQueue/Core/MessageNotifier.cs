namespace LiteQueue.Core;

/// <summary>
/// Async pulse signal. Signal() wakes ALL current waiters by completing the current TCS
/// and replacing it atomically. Uses TaskCompletionSource so N waiters all wake on each signal.
/// This is the async equivalent of Monitor.PulseAll().
/// </summary>
internal sealed class MessageNotifier : IDisposable
{
    private volatile TaskCompletionSource _tcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _disposed;

    /// <summary>Signals all current waiters. Atomically replaces the TCS.</summary>
    public void Signal()
    {
        if (_disposed) return;
        var old = Interlocked.Exchange(
            ref _tcs,
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        old.TrySetResult();
    }

    /// <summary>Waits until the next Signal() call.</summary>
    public async ValueTask WaitAsync(CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _tcs.Task.WaitAsync(ct);
    }

    public void Dispose()
    {
        _disposed = true;
        _tcs.TrySetCanceled();
    }
}
