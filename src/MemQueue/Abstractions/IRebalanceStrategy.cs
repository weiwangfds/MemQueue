namespace MemQueue.Abstractions;

/// <summary>
/// Strategy for rebalancing partition assignments within a consumer group.
/// </summary>
public interface IRebalancer
{
    /// <summary>
    /// Compute new partition assignments given the current set of consumer IDs.
    /// Returns a map of consumerId → assigned partition indices.
    /// </summary>
    IReadOnlyDictionary<string, IReadOnlyList<int>> Assign(
        int partitionCount,
        IReadOnlyList<string> consumerIds);
}
