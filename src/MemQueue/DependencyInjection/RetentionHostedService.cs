using MemQueue.Core;
using Microsoft.Extensions.Hosting;

namespace MemQueue.DependencyInjection;

internal sealed class RetentionHostedService : IHostedService, IAsyncDisposable
{
    private readonly RetentionManager _retentionManager;

    public RetentionHostedService(RetentionManager retentionManager)
    {
        _retentionManager = retentionManager;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _retentionManager.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _retentionManager.StopAsync(cancellationToken);
    }

    public ValueTask DisposeAsync()
    {
        return _retentionManager.DisposeAsync();
    }
}
