// src/LiteQueue/DependencyInjection/ServiceCollectionExtensions.cs
using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation;
using LiteQueue.Implementation.PartitionSelectors;
using LiteQueue.Implementation.RebalanceStrategies;
using LiteQueue.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace LiteQueue.DependencyInjection;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddLiteQueue(
        this IServiceCollection services,
        Action<LiteQueueBuilder>? configure = null)
    {
        var builder = new LiteQueueBuilder(services);
        configure?.Invoke(builder);

        var globalOptions = builder.Options;

        services.TryAddSingleton<ITopicManager>(sp =>
        {
            var tm = new TopicManager();
            tm.SetDefaultOrdering(globalOptions.DefaultOrdering);
            return tm;
        });
        services.TryAddSingleton<GroupCoordinator>();
        services.TryAddSingleton<RetentionManager>();
        services.TryAddSingleton<IPartitioner, RoundRobinPartitioner>();
        services.TryAddSingleton<IRebalancer, RangeRebalancer>();

        // Register Bus as concrete + both interfaces
        services.TryAddSingleton<Bus>();
        services.TryAddSingleton<IMessageBus>(sp => sp.GetRequiredService<Bus>());
        services.TryAddSingleton<IDomainEventBus>(sp => sp.GetRequiredService<Bus>());

        services.AddSingleton(globalOptions);
        services.AddHostedService<TopicInitializationHostedService>();
        services.AddHostedService<RetentionHostedService>();

        return services;
    }
}

internal sealed class TopicInitializationHostedService : IHostedService
{
    private readonly ITopicManager _topicManager;
    private readonly LiteQueueOptions _options;

    public TopicInitializationHostedService(ITopicManager topicManager, LiteQueueOptions options)
    {
        _topicManager = topicManager;
        _options = options;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var (name, opts) in _options.Topics)
        {
            if (_topicManager.TopicExists((TopicId)name)) continue;
            var capturedOpts = opts;
            _topicManager.CreateTopic((TopicId)name, o =>
            {
                o.PartitionCount = capturedOpts.PartitionCount;
                o.BufferCapacity = capturedOpts.BufferCapacity;
                o.OverflowPolicy = capturedOpts.OverflowPolicy;
                o.Retention = capturedOpts.Retention;
                o.Backpressure = capturedOpts.Backpressure;
                o.Ordering = capturedOpts.Ordering;
            });
        }
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
