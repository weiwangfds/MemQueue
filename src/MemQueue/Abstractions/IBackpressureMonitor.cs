namespace MemQueue.Abstractions;

/// <summary>
/// Monitors backpressure state for a topic's partitions.
/// </summary>
public interface IBackpressureMonitor
{
    /// <summary>
    /// Current buffer utilization as a fraction (0.0 to 1.0) for a given partition.
    /// </summary>
    double GetUtilization(string topic, int partition);

    /// <summary>
    /// Consumer lag (headOffset - min committedOffset) for a given partition.
    /// Returns -1 if no consumers exist.
    /// </summary>
    long GetConsumerLag(string topic, int partition);

    /// <summary>
    /// Number of producers currently blocked waiting for space.
    /// </summary>
    int GetBlockedProducerCount(string topic, int partition);

    /// <summary>
    /// Event fired when buffer utilization crosses the high watermark threshold.
    /// Args: (topic, partition, utilizationPercent)
    /// </summary>
    event Action<string, int, double>? HighWatermarkReached;

    /// <summary>
    /// Event fired when buffer utilization drops below the low watermark threshold.
    /// Args: (topic, partition, utilizationPercent)
    /// </summary>
    event Action<string, int, double>? LowWatermarkRecovered;

    /// <summary>
    /// Event fired when a consumer group's lag exceeds the configured threshold.
    /// Args: (topic, partition, groupId, lag)
    /// </summary>
    event Action<string, int, string, long>? SlowConsumerDetected;
}
