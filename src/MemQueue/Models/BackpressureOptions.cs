namespace MemQueue.Models;

/// <summary>
/// Configuration for consumer-aware backpressure behavior.
/// </summary>
public sealed class BackpressureOptions
{
    /// <summary>
    /// Buffer utilization percentage that triggers a high-watermark warning event. Default 80%.
    /// </summary>
    public double HighWatermarkPercent { get; set; } = 0.8;

    /// <summary>
    /// Buffer utilization percentage that triggers a low-watermark recovery event. Default 30%.
    /// </summary>
    public double LowWatermarkPercent { get; set; } = 0.3;

    /// <summary>
    /// Strategy for handling slow consumers. Default: None (no action).
    /// </summary>
    public SlowConsumerStrategy SlowConsumerStrategy { get; set; } = SlowConsumerStrategy.None;

    /// <summary>
    /// Maximum acceptable consumer lag (headOffset - committedOffset) before triggering slow consumer action.
    /// Only applies when SlowConsumerStrategy is not None. Default: 0 (use capacity * 0.5).
    /// Set to a positive value to override.
    /// </summary>
    public long SlowConsumerLagThreshold { get; set; }
}

/// <summary>
/// Action to take when a consumer group falls behind.
/// </summary>
public enum SlowConsumerStrategy
{
    /// <summary>No action — just track metrics.</summary>
    None = 0,
    /// <summary>Fire a SlowConsumerDetected event with lag details.</summary>
    Warn = 1,
    /// <summary>
    /// Disconnect the slow consumer by forcing a higher committed offset,
    /// allowing the buffer to advance past unread data. Data loss possible.
    /// </summary>
    ForceAdvance = 2
}
