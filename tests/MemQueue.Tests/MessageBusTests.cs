using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Implementation.PartitionSelectors;
using MemQueue.Implementation.RebalanceStrategies;
using MemQueue.Models;
using Xunit;

namespace MemQueue.Tests;

public class BusTests
{
    private static Bus CreateBus(TopicManager? tm = null)
    {
        var topicManager = tm ?? new TopicManager();
        var coordinator = new GroupCoordinator();
        return new Bus(topicManager, coordinator, new RoundRobinPartitioner(), new RangeRebalancer());
    }

    [Fact]
    public async Task ProduceAsync_AutoCreatesTopic()
    {
        using var tm = new TopicManager();
        await using var bus = CreateBus(tm);

        var result = await bus.ProduceAsync((TopicId)"new-topic", new TestMessage("hello"));

        Assert.True(result.Offset.Value >= 0);
        Assert.Equal((TopicId)"new-topic", result.Topic);
        Assert.True(tm.TopicExists((TopicId)"new-topic"));
    }

    [Fact]
    public async Task SubscribeAsync_BroadcastReceivesMessages()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"broadcast-topic");
        await using var bus = CreateBus(tm);

        var received = new List<string>();
        var firstMessageConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var subscribeTask = bus.SubscribeAsync<TestMessage>(
            (TopicId)"broadcast-topic",
            (msg, ctx, ct) =>
            {
                received.Add(msg.Value);
                firstMessageConsumed.TrySetResult();
                if (received.Count >= 2) cts.Cancel();
                return ValueTask.CompletedTask;
            },
            cts.Token);

        // Produce first message and wait for consumer to actually process it
        await bus.ProduceAsync((TopicId)"broadcast-topic", new TestMessage("msg1"));
        await firstMessageConsumed.Task.WaitAsync(cts.Token);

        await bus.ProduceAsync((TopicId)"broadcast-topic", new TestMessage("msg2"));

        // Wait until we've received both or timeout
        using var doneCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (received.Count < 2 && !doneCts.Token.IsCancellationRequested)
            await Task.Delay(50, doneCts.Token);

        cts.Cancel();
        await subscribeTask;

        Assert.Equal(2, received.Count);
        Assert.Contains("msg1", received);
        Assert.Contains("msg2", received);
    }

    [Fact]
    public async Task SubscribeAsync_ConsumerGroup_Distributes()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"group-topic", o => o.PartitionCount = 4);
        await using var bus = CreateBus(tm);

        var received1 = new List<string>();
        var received2 = new List<string>();
        var totalReceived = 0;
        var allConsumed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var task1 = bus.SubscribeAsync<TestMessage>(
            (TopicId)"group-topic", (ConsumerGroupId)"my-group",
            (msg, ctx, ct) =>
            {
                received1.Add(msg.Value);
                if (Interlocked.Increment(ref totalReceived) >= 4) allConsumed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            cts.Token);

        var task2 = bus.SubscribeAsync<TestMessage>(
            (TopicId)"group-topic", (ConsumerGroupId)"my-group",
            (msg, ctx, ct) =>
            {
                received2.Add(msg.Value);
                if (Interlocked.Increment(ref totalReceived) >= 4) allConsumed.TrySetResult();
                return ValueTask.CompletedTask;
            },
            cts.Token);

        for (int i = 0; i < 4; i++)
            await bus.ProduceAsync((TopicId)"group-topic", new TestMessage($"msg-{i}"));

        await allConsumed.Task.WaitAsync(cts.Token);

        cts.Cancel();
        await Task.WhenAll(task1, task2);

        Assert.Equal(4, received1.Count + received2.Count);
    }

    [Fact]
    public async Task SubscribeAsync_AutoCommit()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"commit-topic", o => o.PartitionCount = 1);
        var coordinator = new GroupCoordinator();
        await using var bus = new Bus(tm, coordinator, new RoundRobinPartitioner(), new RangeRebalancer());

        long committedOffset = -2;
        var handlerDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var task = bus.SubscribeAsync<TestMessage>(
            (TopicId)"commit-topic", (ConsumerGroupId)"commit-group", autoCommit: true,
            (msg, ctx, ct) =>
            {
                committedOffset = ctx.Offset;
                handlerDone.SetResult();
                return ValueTask.CompletedTask;
            },
            cts.Token);

        await bus.ProduceAsync((TopicId)"commit-topic", new TestMessage("auto-commit-msg"));

        await handlerDone.Task.WaitAsync(cts.Token);
        await Task.Delay(50);

        var storedOffset = coordinator.GetCommittedOffset((ConsumerGroupId)"commit-group", new PartitionId(0));
        Assert.Equal(committedOffset, storedOffset.Value);

        cts.Cancel();
        await task;
    }

    [Fact]
    public async Task CreateProducer_ReturnsTypedProducer()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        await using var bus = CreateBus(tm);

        var producer = bus.CreateProducer<TestMessage>((TopicId)"test");

        Assert.NotNull(producer);
        Assert.Equal((TopicId)"test", producer.Topic);
        await producer.DisposeAsync();
    }

    [Fact]
    public async Task CreateConsumer_ReturnsTypedConsumer()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        await using var bus = CreateBus(tm);

        var consumer = bus.CreateConsumer<TestMessage>((TopicId)"test");

        Assert.NotNull(consumer);
        Assert.Equal((TopicId)"test", consumer.Topic);
        Assert.Null(consumer.GroupId);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_CleansUp()
    {
        var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        await bus.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await bus.ProduceAsync((TopicId)"test", new TestMessage("x")));
    }
}
