namespace LiteQueue.Models;

/// <summary>
/// Defines the behavior when a consumer has no committed offset or the offset is out of range.
/// Mirrors Kafka's auto.offset.reset configuration.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>
    /// Start consuming from the latest offset (new messages only). Kafka default.
    /// </summary>
    Latest = 0,

    /// <summary>
    /// Start consuming from the earliest available offset.
    /// </summary>
    Earliest = 1,

    /// <summary>
    /// Throw an exception if no offset is found.
    /// </summary>
    Error = 2
}
