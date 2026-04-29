using LiteQueue.Abstractions;

namespace LiteQueue.Implementation.PartitionSelectors;

/// <summary>
/// Selects partitions randomly.
/// </summary>
public sealed class RandomPartitioner : IPartitioner
{
    /// <summary>
    /// Selects a random partition.
    /// </summary>
    public int SelectPartition(int partitionCount, string? key, ReadOnlySpan<byte> keyBytes)
    {
        return Random.Shared.Next(partitionCount);
    }
}
