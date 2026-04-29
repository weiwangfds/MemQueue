using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Implementation.PartitionSelectors;
using MemQueue.Implementation.RebalanceStrategies;
using MemQueue.Models;
using Microsoft.Extensions.DependencyInjection;

namespace MemQueue.DependencyInjection;

/// <summary>
/// Builder for configuring MemQueue services.
/// </summary>
public sealed class MemQueueBuilder
{
    internal IServiceCollection Services { get; }
    internal MemQueueOptions Options { get; } = new();

    /// <summary>
    /// Initializes a new instance of the MemQueueBuilder class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public MemQueueBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Adds a topic to the configuration.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public MemQueueBuilder AddTopic(string name, Action<TopicOptions>? configure = null)
    {
        var options = new TopicOptions();
        configure?.Invoke(options);
        options.Ordering ??= Options.DefaultOrdering;
        Options.Topics[name] = options;
        return this;
    }

    public MemQueueBuilder SetDefaultOrdering(OrderingMode mode)
    {
        Options.DefaultOrdering = mode;
        return this;
    }

    /// <summary>
    /// Configures the round-robin partitioner.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public MemQueueBuilder UseRoundRobinPartitioner()
    {
        Services.AddSingleton<IPartitioner, RoundRobinPartitioner>();
        return this;
    }

    /// <summary>
    /// Configures the key hash partitioner.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public MemQueueBuilder UseKeyHashPartitioner()
    {
        Services.AddSingleton<IPartitioner, KeyHashPartitioner>();
        return this;
    }

    /// <summary>
    /// Configures the range rebalancer.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public MemQueueBuilder UseRangeRebalancer()
    {
        Services.AddSingleton<IRebalancer, RangeRebalancer>();
        return this;
    }

    /// <summary>
    /// Configures the round-robin rebalancer.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public MemQueueBuilder UseRoundRobinRebalancer()
    {
        Services.AddSingleton<IRebalancer, RoundRobinRebalancer>();
        return this;
    }
}
