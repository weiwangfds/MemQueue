using MemQueue.Abstractions;

namespace MemQueue.Implementation.PartitionSelectors;

/// <summary>
/// Selects partitions in a round-robin fashion.
/// </summary>
public sealed class RoundRobinPartitioner : IPartitioner
{
    private int _counter;

    /// <summary>
    /// Selects the next partition using round-robin.
    /// </summary>
    public int SelectPartition(int partitionCount, string? key, ReadOnlySpan<byte> keyBytes)
    {
        return Interlocked.Increment(ref _counter) % partitionCount;
    }
}
