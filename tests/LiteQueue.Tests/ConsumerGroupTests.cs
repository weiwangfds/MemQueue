using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation.RebalanceStrategies;
using LiteQueue.Models;
using Xunit;

namespace LiteQueue.Tests;

public class ConsumerGroupTests
{
    private static RangeRebalancer CreateStrategy() => new();

    [Fact]
    public void RegisterConsumer_AssignsPartitions()
    {
        using var coordinator = new GroupCoordinator();
        var strategy = CreateStrategy();

        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-a", strategy, 6);
        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-b", strategy, 6);

        var partitionsA = coordinator.GetAssignedPartitions((ConsumerGroupId)"group1", (ConsumerId)"consumer-a");
        var partitionsB = coordinator.GetAssignedPartitions((ConsumerGroupId)"group1", (ConsumerId)"consumer-b");

        Assert.Equal(3, partitionsA.Count);
        Assert.Equal(3, partitionsB.Count);

        var allAssigned = partitionsA.Concat(partitionsB).Select(p => p.Value).OrderBy(p => p).ToList();
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, allAssigned);
    }

    [Fact]
    public void UnregisterConsumer_Rebalances()
    {
        using var coordinator = new GroupCoordinator();
        var strategy = CreateStrategy();

        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-a", strategy, 6);
        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-b", strategy, 6);
        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-c", strategy, 6);

        coordinator.UnregisterConsumer((ConsumerGroupId)"group1", (ConsumerId)"consumer-b", 6);

        var partitionsA = coordinator.GetAssignedPartitions((ConsumerGroupId)"group1", (ConsumerId)"consumer-a");
        var partitionsC = coordinator.GetAssignedPartitions((ConsumerGroupId)"group1", (ConsumerId)"consumer-c");
        var partitionsB = coordinator.GetAssignedPartitions((ConsumerGroupId)"group1", (ConsumerId)"consumer-b");

        Assert.Equal(6, partitionsA.Count + partitionsC.Count);
        Assert.Empty(partitionsB);
    }

    [Fact]
    public void CommitOffset_Tracked()
    {
        using var coordinator = new GroupCoordinator();
        var strategy = CreateStrategy();

        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-a", strategy, 4);
        coordinator.CommitOffset((ConsumerGroupId)"group1", new PartitionId(0), new Offset(42));

        Assert.Equal(42L, coordinator.GetCommittedOffset((ConsumerGroupId)"group1", new PartitionId(0)));
    }

    [Fact]
    public void GetCommittedOffset_DefaultMinusOne()
    {
        using var coordinator = new GroupCoordinator();
        var strategy = CreateStrategy();

        coordinator.RegisterConsumer((ConsumerGroupId)"group1", (TopicId)"topic", (ConsumerId)"consumer-a", strategy, 4);

        Assert.Equal(-1L, coordinator.GetCommittedOffset((ConsumerGroupId)"group1", new PartitionId(0)));
    }

    [Fact]
    public void GetCommittedOffset_UnknownGroup_ReturnsMinusOne()
    {
        using var coordinator = new GroupCoordinator();
        Assert.Equal(-1L, coordinator.GetCommittedOffset((ConsumerGroupId)"unknown", new PartitionId(0)));
    }

    [Fact]
    public void GetAssignedPartitions_UnknownGroup_ReturnsEmpty()
    {
        using var coordinator = new GroupCoordinator();
        Assert.Empty(coordinator.GetAssignedPartitions((ConsumerGroupId)"unknown", (ConsumerId)"consumer-x"));
    }
}
