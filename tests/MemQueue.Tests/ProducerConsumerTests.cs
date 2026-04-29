using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Implementation.PartitionSelectors;
using MemQueue.Models;
using Xunit;

namespace MemQueue.Tests;

public class ProducerConsumerTests
{
    private static TopicManager CreateTopicManager(int partitions = 4)
    {
        var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test-topic", o => o.PartitionCount = partitions);
        return tm;
    }

    [Fact]
    public async Task Producer_ProduceAsync_ReturnsOffset()
    {
        using var tm = CreateTopicManager();
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);

        var result = await producer.ProduceAsync(new TestMessage("hello"));

        Assert.True(result.Offset.Value >= 0);
        Assert.Equal((TopicId)"test-topic", result.Topic);
    }

    [Fact]
    public async Task Producer_ProduceAsync_WithKey()
    {
        using var tm = CreateTopicManager(4);
        var selector = new KeyHashPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);

        var r1 = await producer.ProduceAsync(new TestMessage("a"), key: "my-key");
        var r2 = await producer.ProduceAsync(new TestMessage("b"), key: "my-key");

        Assert.Equal(r1.Partition, r2.Partition);
    }

    [Fact]
    public async Task Producer_ProduceBatchAsync()
    {
        using var tm = CreateTopicManager();
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);

        var messages = Enumerable.Range(0, 5).Select(i => new TestMessage($"msg-{i}")).ToList();
        var results = await producer.ProduceBatchAsync(messages);

        Assert.Equal(5, results.Count);
        Assert.All(results, r => Assert.True(r.Offset.Value >= 0));
    }

    [Fact]
    public async Task Consumer_ConsumeAsync_ReceivesMessage()
    {
        using var tm = CreateTopicManager(1);
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);
        var consumer = new Consumer<TestMessage>((TopicId)"test-topic", null, tm, null);

        await producer.ProduceAsync(new TestMessage("hello"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await consumer.ConsumeAsync(cts.Token);

        Assert.Equal("hello", result.Value.Value);
        Assert.Equal((TopicId)"test-topic", result.Topic);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task Consumer_ConsumeAllAsync_StreamsMessages()
    {
        using var tm = CreateTopicManager(1);
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);
        var consumer = new Consumer<TestMessage>((TopicId)"test-topic", null, tm, null);

        await producer.ProduceAsync(new TestMessage("a"));
        await producer.ProduceAsync(new TestMessage("b"));
        await producer.ProduceAsync(new TestMessage("c"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var messages = new List<string>();
        await foreach (var r in consumer.ConsumeAllAsync(cts.Token))
        {
            messages.Add(r.Value.Value);
            if (messages.Count == 3)
            {
                cts.Cancel();
                break;
            }
        }

        Assert.Equal(new[] { "a", "b", "c" }, messages);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task Consumer_SeekAsync_ReplaysMessages()
    {
        using var tm = CreateTopicManager(1);
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);
        var consumer = new Consumer<TestMessage>((TopicId)"test-topic", null, tm, null);

        await producer.ProduceAsync(new TestMessage("first"));
        await producer.ProduceAsync(new TestMessage("second"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var r1 = await consumer.ConsumeAsync(cts.Token);
        Assert.Equal("first", r1.Value.Value);

        await consumer.SeekAsync(new PartitionId(0), new Offset(0));

        var r2 = await consumer.ConsumeAsync(cts.Token);
        Assert.Equal("first", r2.Value.Value);

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task Consumer_BroadcastMode_ReceivesAllMessages()
    {
        using var tm = CreateTopicManager(1);
        var selector = new RoundRobinPartitioner();
        var producer = new Producer<TestMessage>((TopicId)"test-topic", tm, selector);
        var consumer1 = new Consumer<TestMessage>((TopicId)"test-topic", null, tm, null);
        var consumer2 = new Consumer<TestMessage>((TopicId)"test-topic", null, tm, null);

        await producer.ProduceAsync(new TestMessage("broadcast"));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var r1 = await consumer1.ConsumeAsync(cts.Token);
        var r2 = await consumer2.ConsumeAsync(cts.Token);

        Assert.Equal("broadcast", r1.Value.Value);
        Assert.Equal("broadcast", r2.Value.Value);

        await consumer1.DisposeAsync();
        await consumer2.DisposeAsync();
    }
}
