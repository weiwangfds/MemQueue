using LiteQueue.Abstractions;

namespace LiteQueue.Implementation.RebalanceStrategies;

/// <summary>
/// Rebalances partitions using round-robin assignment.
/// </summary>
public sealed class RoundRobinRebalancer : IRebalancer
{
    /// <summary>
    /// Assigns partitions to consumers using round-robin strategy.
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

        foreach (var id in sorted)
            result[id] = new List<int>();

        for (int p = 0; p < partitionCount; p++)
        {
            var consumerId = sorted[p % sorted.Count];
            ((List<int>)result[consumerId]).Add(p);
        }

        return result;
    }
}
