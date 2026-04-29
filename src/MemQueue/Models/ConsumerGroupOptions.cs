namespace MemQueue.Models;

/// <summary>
/// Configuration options for a consumer group.
/// </summary>
public sealed class ConsumerGroupOptions
{
    /// <summary>
    /// Gets the consumer group identifier.
    /// </summary>
    public required string GroupId { get; init; }

    /// <summary>
    /// Gets the topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets a value indicating whether auto-commit is enabled.
    /// </summary>
    public bool AutoCommit { get; init; } = true;

    /// <summary>
    /// Gets the offset reset behavior when no initial offset is present.
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Latest;
}
