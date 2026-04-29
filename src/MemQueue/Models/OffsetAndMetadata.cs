namespace MemQueue.Models;

/// <summary>
/// Represents an offset to commit along with optional metadata.
/// Mirrors Kafka's OffsetAndMetadata.
/// </summary>
public sealed class OffsetAndMetadata
{
    public required long Offset { get; init; }
    public string? Metadata { get; init; }

    public OffsetAndMetadata(long offset, string? metadata = null)
    {
        Offset = offset;
        Metadata = metadata;
    }
}
