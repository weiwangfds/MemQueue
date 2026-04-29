using MemQueue.Abstractions;

namespace MemQueue.Implementation.RebalanceStrategies;

/// <summary>
/// Rebalances partitions using range assignment.
/// </summary>
public sealed class RangeRebalancer : IRebalancer
{
    /// <summary>
    /// Assigns partitions to consumers using range strategy.
    /// </summary>
    public IReadOnlyDictionary<string, IReadOnlyList<int>> Assign(
        int partitionCount,
        IReadOnlyList<string> consumerIds)
    {
        if (consumerIds.Count == 0)
            return new Dictionary<string, IReadOnlyList<int>>();

        var sorted = new List<string>(consumerIds);
        sorted.Sort(StringComparer.Ordinal);
        var result = new Dictionary<string, IReadOnlyList<int>>(sorted.Count);

        int baseCount = partitionCount / sorted.Count;
        int extra = partitionCount % sorted.Count;
        int offset = 0;

        for (int i = 0; i < sorted.Count; i++)
        {
            int count = baseCount + (i < extra ? 1 : 0);
            var partitions = new int[count];
            for (int j = 0; j < count; j++)
            {
                partitions[j] = offset + j;
            }
            result[sorted[i]] = partitions;
            offset += count;
        }

        return result;
    }
}
