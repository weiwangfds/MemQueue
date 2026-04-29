namespace LiteQueue.Models;

/// <summary>
/// Statistics for an entire topic including all partitions.
/// </summary>
public sealed class TopicStatistics
{
    public required string Topic { get; init; }
    public required int PartitionCount { get; init; }
    public required long TotalMessages { get; init; }
    public required double AverageUtilization { get; init; }
    public required Dictionary<int, PartitionStats> Partitions { get; init; }
}

/// <summary>
/// Statistics for a single partition within a topic.
/// </summary>
public sealed class PartitionStats
{
    public required int PartitionId { get; init; }
    public required long HeadOffset { get; init; }
    public required long TailOffset { get; init; }
    public required long MessageCount { get; init; }
    public required double Utilization { get; init; }
    public long ConsumerLag { get; init; }
}
