using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation;
using LiteQueue.Implementation.PartitionSelectors;
using LiteQueue.Implementation.RebalanceStrategies;
using LiteQueue.Models;
using Microsoft.Extensions.DependencyInjection;

namespace LiteQueue.DependencyInjection;

/// <summary>
/// Builder for configuring LiteQueue services.
/// </summary>
public sealed class LiteQueueBuilder
{
    internal IServiceCollection Services { get; }
    internal LiteQueueOptions Options { get; } = new();

    /// <summary>
    /// Initializes a new instance of the LiteQueueBuilder class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    public LiteQueueBuilder(IServiceCollection services)
    {
        Services = services;
    }

    /// <summary>
    /// Adds a topic to the configuration.
    /// </summary>
    /// <param name="name">The topic name.</param>
    /// <param name="configure">Optional configuration action.</param>
    /// <returns>The builder for chaining.</returns>
    public LiteQueueBuilder AddTopic(string name, Action<TopicOptions>? configure = null)
    {
        var options = new TopicOptions();
        configure?.Invoke(options);
        options.Ordering ??= Options.DefaultOrdering;
        Options.Topics[name] = options;
        return this;
    }

    public LiteQueueBuilder SetDefaultOrdering(OrderingMode mode)
    {
        Options.DefaultOrdering = mode;
        return this;
    }

    /// <summary>
    /// Configures the round-robin partitioner.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public LiteQueueBuilder UseRoundRobinPartitioner()
    {
        Services.AddSingleton<IPartitioner, RoundRobinPartitioner>();
        return this;
    }

    /// <summary>
    /// Configures the key hash partitioner.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public LiteQueueBuilder UseKeyHashPartitioner()
    {
        Services.AddSingleton<IPartitioner, KeyHashPartitioner>();
        return this;
    }

    /// <summary>
    /// Configures the range rebalancer.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public LiteQueueBuilder UseRangeRebalancer()
    {
        Services.AddSingleton<IRebalancer, RangeRebalancer>();
        return this;
    }

    /// <summary>
    /// Configures the round-robin rebalancer.
    /// </summary>
    /// <returns>The builder for chaining.</returns>
    public LiteQueueBuilder UseRoundRobinRebalancer()
    {
        Services.AddSingleton<IRebalancer, RoundRobinRebalancer>();
        return this;
    }
}
