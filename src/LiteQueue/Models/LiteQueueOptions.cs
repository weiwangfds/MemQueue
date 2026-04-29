using LiteQueue.Abstractions;

namespace LiteQueue.Models;

/// <summary>
/// Configuration options for LiteQueue.
/// </summary>
public sealed class LiteQueueOptions
{
    internal Dictionary<string, TopicOptions> Topics { get; } = new();

    public OrderingMode DefaultOrdering { get; set; } = OrderingMode.None;
}

/// <summary>
/// Configuration options for a topic.
/// </summary>
public sealed class TopicOptions
{
    /// <summary>
    /// Gets or sets the number of partitions for the topic.
    /// </summary>
    public int PartitionCount { get; set; } = 1;

    /// <summary>
    /// Gets or sets the buffer capacity for the topic.
    /// </summary>
    public int BufferCapacity { get; set; } = 1024;

    /// <summary>
    /// Gets or sets the retention policy for the topic.
    /// </summary>
    public RetentionPolicy? Retention { get; set; }

    /// <summary>
    /// Gets or sets the overflow policy for the topic.
    /// </summary>
    public OverflowPolicy OverflowPolicy { get; set; } = OverflowPolicy.OverwriteOldest;

    /// <summary>
    /// Gets or sets the backpressure options for the topic.
    /// </summary>
    public BackpressureOptions? Backpressure { get; set; }

    public OrderingMode? Ordering { get; set; }
}

/// <summary>
/// Extension methods for TopicOptions.
/// </summary>
public static class TopicOptionsExtensions
{
    /// <summary>
    /// Configures the number of partitions for the topic.
    /// </summary>
    /// <param name="options">The topic options.</param>
    /// <param name="count">The number of partitions.</param>
    /// <returns>The topic options for chaining.</returns>
    public static TopicOptions WithPartitions(this TopicOptions options, int count)
    {
        options.PartitionCount = count;
        return options;
    }

    /// <summary>
    /// Configures the buffer capacity for the topic.
    /// </summary>
    /// <param name="options">The topic options.</param>
    /// <param name="capacity">The buffer capacity.</param>
    /// <returns>The topic options for chaining.</returns>
    public static TopicOptions WithBufferCapacity(this TopicOptions options, int capacity)
    {
        options.BufferCapacity = capacity;
        return options;
    }

    /// <summary>
    /// Configures the retention policy for the topic.
    /// </summary>
    /// <param name="options">The topic options.</param>
    /// <param name="policy">The retention policy.</param>
    /// <returns>The topic options for chaining.</returns>
    public static TopicOptions WithRetention(this TopicOptions options, RetentionPolicy policy)
    {
        options.Retention = policy;
        return options;
    }

    /// <summary>
    /// Configures the overflow policy for the topic.
    /// </summary>
    /// <param name="options">The topic options.</param>
    /// <param name="policy">The overflow policy.</param>
    /// <returns>The topic options for chaining.</returns>
    public static TopicOptions WithOverflowPolicy(this TopicOptions options, OverflowPolicy policy)
    {
        options.OverflowPolicy = policy;
        return options;
    }

    /// <summary>
    /// Configures the backpressure options for the topic.
    /// </summary>
    /// <param name="options">The topic options.</param>
    /// <param name="configure">The configuration action.</param>
    /// <returns>The topic options for chaining.</returns>
    public static TopicOptions WithBackpressure(this TopicOptions options, Action<BackpressureOptions> configure)
    {
        var bp = new BackpressureOptions();
        configure(bp);
        options.Backpressure = bp;
        return options;
    }

    public static TopicOptions WithOrdering(this TopicOptions options, OrderingMode mode)
    {
        options.Ordering = mode;
        return options;
    }
}
