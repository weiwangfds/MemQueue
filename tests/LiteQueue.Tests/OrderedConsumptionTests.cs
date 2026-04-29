using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation;
using LiteQueue.Implementation.PartitionSelectors;
using LiteQueue.Implementation.RebalanceStrategies;
using LiteQueue.Models;
using Xunit;

namespace LiteQueue.Tests;

public record SequencedMessage(int Sequence, string Group) : MessageBase;

public class OrderedConsumptionTests
{
    private static TopicManager CreateTopicManager(int partitions, OrderingMode ordering)
    {
        var tm = new TopicManager();
        tm.CreateTopic((TopicId)"orders", o =>
        {
            o.PartitionCount = partitions;
            o.Ordering = ordering;
        });
        return tm;
    }

    [Fact]
    public async Task PerKey_SameKey_MessagesGoToSamePartition()
    {
        using var tm = CreateTopicManager(4, OrderingMode.PerKey);
        var partitioner = new KeyHashPartitioner();
        var producer = new Producer<SequencedMessage>((TopicId)"orders", tm, partitioner);

        var results = new List<DeliveryResult>();
        for (var i = 0; i < 10; i++)
        {
            var r = await producer.ProduceAsync(new SequencedMessage(i, "order-A"), key: "order-A");
            results.Add(r);
        }

        var partitions = results.Select(r => r.Partition).Distinct().ToList();
        Assert.Single(partitions);
    }

    [Fact]
    public async Task PerKey_DifferentKeys_CanGoToDifferentPartitions()
    {
        using var tm = CreateTopicManager(4, OrderingMode.PerKey);
        var partitioner = new KeyHashPartitioner();
        var producer = new Producer<SequencedMessage>((TopicId)"orders", tm, partitioner);

        var r1 = await producer.ProduceAsync(new SequencedMessage(0, "A"), key: "key-A");
        var r2 = await producer.ProduceAsync(new SequencedMessage(0, "B"), key: "key-B");
        var r3 = await producer.ProduceAsync(new SequencedMessage(0, "C"), key: "key-C");
        var r4 = await producer.ProduceAsync(new SequencedMessage(0, "D"), key: "key-D");

        var partitions = new[] { r1, r2, r3, r4 }.Select(r => r.Partition).Distinct().ToList();
        Assert.True(partitions.Count >= 2, "With 4 different keys and 4 partitions, at least 2 partitions should be used");
    }

    [Fact]
    public async Task PerKey_SinglePartition_ConsumedInOrder()
    {
        using var tm = CreateTopicManager(1, OrderingMode.PerKey);
        var partitioner = new KeyHashPartitioner();
        var producer = new Producer<SequencedMessage>((TopicId)"orders", tm, partitioner);

        for (var i = 0; i < 20; i++)
        {
            await producer.ProduceAsync(new SequencedMessage(i, "seq"), key: "seq");
        }

        var consumer = new Consumer<SequencedMessage>((TopicId)"orders", null, tm, null);
        var consumed = new List<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var r in consumer.ConsumeAllAsync(cts.Token))
        {
            consumed.Add(r.Value.Sequence);
            if (consumed.Count == 20) { cts.Cancel(); break; }
        }

        Assert.Equal(Enumerable.Range(0, 20).ToList(), consumed);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task PerKey_MultiplePartitions_SameKeyConsumedInOrder()
    {
        using var tm = CreateTopicManager(4, OrderingMode.PerKey);
        var partitioner = new KeyHashPartitioner();
        var producer = new Producer<SequencedMessage>((TopicId)"orders", tm, partitioner);

        // Produce 15 messages all with same key → same partition
        const int count = 15;
        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(new SequencedMessage(i, "order-X"), key: "order-X");
        }

        var consumer = new Consumer<SequencedMessage>((TopicId)"orders", null, tm, null);
        var consumed = new List<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var r in consumer.ConsumeAllAsync(cts.Token))
        {
            consumed.Add(r.Value.Sequence);
            if (consumed.Count == count) { cts.Cancel(); break; }
        }

        // Verify strict ordering
        Assert.Equal(count, consumed.Count);
        for (var i = 0; i < count; i++)
        {
            Assert.Equal(i, consumed[i]);
        }

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task Bus_PerKey_ForceKeyHashPartitioning()
    {
        // Test that Bus.ProduceAsync enforces key-hash partitioning
        // even when default partitioner is RoundRobin
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"orders", o =>
        {
            o.PartitionCount = 4;
            o.Ordering = OrderingMode.PerKey;
        });

        var coordinator = new GroupCoordinator();
        var defaultPartitioner = new RoundRobinPartitioner(); // ignores keys
        var rebalancer = new RangeRebalancer();
        await using var bus = new Bus(tm, coordinator, defaultPartitioner, rebalancer);

        // Produce 10 messages with same key via Bus
        var results = new List<DeliveryResult>();
        for (var i = 0; i < 10; i++)
        {
            var r = await bus.ProduceAsync((TopicId)"orders", new SequencedMessage(i, "X"), key: "same-key");
            results.Add(r);
        }

        // All should go to same partition because PerKey overrides RoundRobin
        var partitions = results.Select(r => r.Partition).Distinct().ToList();
        Assert.Single(partitions);

        coordinator.Dispose();
    }

    [Fact]
    public async Task Bus_PerKey_ConsumeInOrder()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"orders", o =>
        {
            o.PartitionCount = 4;
            o.Ordering = OrderingMode.PerKey;
        });

        var coordinator = new GroupCoordinator();
        var defaultPartitioner = new RoundRobinPartitioner();
        var rebalancer = new RangeRebalancer();
        await using var bus = new Bus(tm, coordinator, defaultPartitioner, rebalancer);

        const int count = 20;
        for (var i = 0; i < count; i++)
        {
            await bus.ProduceAsync((TopicId)"orders", new SequencedMessage(i, "order"), key: "order-key");
        }

        // Subscribe and collect
        var consumed = new List<int>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await bus.SubscribeAsync<SequencedMessage>((TopicId)"orders", async (msg, ctx, ct) =>
        {
            consumed.Add(msg.Sequence);
            if (consumed.Count == count) cts.Cancel();
        }, cts.Token);

        // Wait for all messages
        await Task.Delay(2000);

        Assert.Equal(count, consumed.Count);
        for (var i = 0; i < count; i++)
        {
            Assert.Equal(i, consumed[i]);
        }

        coordinator.Dispose();
    }
}
