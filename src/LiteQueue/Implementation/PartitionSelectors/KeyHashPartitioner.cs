using LiteQueue.Abstractions;

namespace LiteQueue.Implementation.PartitionSelectors;

/// <summary>
/// FNV-1a based partition selector — deterministic, zero-allocation, good distribution.
/// Replaces the previous SHA256 implementation which allocated 32 bytes per call.
/// </summary>
public sealed class KeyHashPartitioner : IPartitioner
{
    private int _roundRobinCounter;

    private const uint FnvOffsetBasis = 2166136261u;
    private const uint FnvPrime = 16777619u;

    public int SelectPartition(int partitionCount, string? key, ReadOnlySpan<byte> keyBytes)
    {
        if (key is null && keyBytes.IsEmpty)
        {
            return Interlocked.Increment(ref _roundRobinCounter) % partitionCount;
        }

        if (!keyBytes.IsEmpty)
        {
            return (int)(HashSpan(keyBytes) % (uint)partitionCount);
        }

        return (int)(HashString(key!) % (uint)partitionCount);
    }

    private static uint HashString(string value)
    {
        var hash = FnvOffsetBasis;
        foreach (var c in value)
        {
            hash ^= (uint)c;
            hash *= FnvPrime;
        }
        return hash;
    }

    private static uint HashSpan(ReadOnlySpan<byte> bytes)
    {
        var hash = FnvOffsetBasis;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= FnvPrime;
        }
        return hash;
    }
}
