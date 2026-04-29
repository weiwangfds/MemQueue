namespace MemQueue.Abstractions;

/// <summary>
/// Strategy for selecting which partition a message goes to.
/// </summary>
public interface IPartitioner
{
    /// <summary>
    /// Select a partition for the given message.
    /// </summary>
    int SelectPartition(int partitionCount, string? key, ReadOnlySpan<byte> keyBytes);
}
