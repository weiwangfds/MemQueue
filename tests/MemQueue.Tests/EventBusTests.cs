using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Implementation.PartitionSelectors;
using MemQueue.Implementation.RebalanceStrategies;
using MemQueue.Models;
using Xunit;

namespace MemQueue.Tests;

public record TestEvent(string Value) : MessageBase;

public class EventBusTests
{
    private static Bus CreateBus()
    {
        var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        tm.CreateTopic((TopicId)"topic-a");
        tm.CreateTopic((TopicId)"topic-b");
        return new Bus(tm, new GroupCoordinator(), new KeyHashPartitioner(), new RangeRebalancer());
    }

    [Fact]
    public async Task OnPublish_HandlerReceivesMessage()
    {
        await using var bus = CreateBus();
        TestEvent? received = null;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) =>
        {
            received = msg;
            return ValueTask.CompletedTask;
        });

        await bus.ProduceAsync((TopicId)"test", new TestEvent("hello"));

        Assert.NotNull(received);
        Assert.Equal("hello", received.Value);
    }

    [Fact]
    public async Task OnPublish_HandlerReceivesCorrectContext()
    {
        await using var bus = CreateBus();
        MessageContext? receivedCtx = null;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) =>
        {
            receivedCtx = ctx;
            return ValueTask.CompletedTask;
        });

        var result = await bus.ProduceAsync((TopicId)"test", new TestEvent("ctx-test"), "my-key");

        Assert.NotNull(receivedCtx);
        Assert.Equal((TopicId)"test", receivedCtx.Topic);
        Assert.Equal(result.Partition, receivedCtx.Partition);
        Assert.Equal(result.Offset, receivedCtx.Offset);
        Assert.Equal("my-key", receivedCtx.Key);
    }

    [Fact]
    public async Task OnPublish_MultipleHandlers_AllInvoked()
    {
        await using var bus = CreateBus();
        var count = 0;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { Interlocked.Increment(ref count); return ValueTask.CompletedTask; });
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { Interlocked.Increment(ref count); return ValueTask.CompletedTask; });
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { Interlocked.Increment(ref count); return ValueTask.CompletedTask; });

        await bus.ProduceAsync((TopicId)"test", new TestEvent("multi"));

        Assert.Equal(3, Volatile.Read(ref count));
    }

    [Fact]
    public async Task OnPublish_HandlerFailure_DoesNotAffectPublish()
    {
        await using var bus = CreateBus();
        var secondCalled = false;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => throw new InvalidOperationException("handler error"));
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { secondCalled = true; return ValueTask.CompletedTask; });

        var result = await bus.ProduceAsync((TopicId)"test", new TestEvent("fail-test"));

        Assert.True(secondCalled);
        Assert.True(result.Offset.Value >= 0);
    }

    [Fact]
    public async Task OnPublish_HandlerFailure_DoesNotAffectOtherHandlers()
    {
        await using var bus = CreateBus();
        var secondCalled = false;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => throw new InvalidOperationException("fail"));
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { secondCalled = true; return ValueTask.CompletedTask; });

        await bus.ProduceAsync((TopicId)"test", new TestEvent("isolation"));

        Assert.True(secondCalled);
    }

    [Fact]
    public async Task OnPublish_AsyncHandler_Awaited()
    {
        await using var bus = CreateBus();
        var sequence = new List<int>();

        bus.OnPublish<TestEvent>((TopicId)"test", async (msg, ctx, ct) =>
        {
            sequence.Add(1);
            await Task.Delay(10, ct);
            sequence.Add(2);
        });

        await bus.ProduceAsync((TopicId)"test", new TestEvent("async"));

        Assert.Equal(new[] { 1, 2 }, sequence);
    }

    [Fact]
    public async Task RemoveHandler_StopsReceivingMessages()
    {
        await using var bus = CreateBus();
        var count = 0;

        Func<TestEvent, MessageContext, CancellationToken, ValueTask> handler = (msg, ctx, ct) =>
        {
            Interlocked.Increment(ref count);
            return ValueTask.CompletedTask;
        };

        bus.OnPublish((TopicId)"test", handler);
        await bus.ProduceAsync((TopicId)"test", new TestEvent("before"));
        Assert.Equal(1, Volatile.Read(ref count));

        var removed = bus.RemoveHandler((TopicId)"test", handler);
        Assert.True(removed);

        await bus.ProduceAsync((TopicId)"test", new TestEvent("after"));
        Assert.Equal(1, Volatile.Read(ref count));
    }

    [Fact]
    public async Task OnPublish_DifferentTopics_Isolated()
    {
        await using var bus = CreateBus();
        var received = new List<string>();

        bus.OnPublish<TestEvent>((TopicId)"topic-a", (msg, ctx, ct) => { received.Add("a:" + msg.Value); return ValueTask.CompletedTask; });
        bus.OnPublish<TestEvent>((TopicId)"topic-b", (msg, ctx, ct) => { received.Add("b:" + msg.Value); return ValueTask.CompletedTask; });

        await bus.ProduceAsync((TopicId)"topic-a", new TestEvent("1"));
        await bus.ProduceAsync((TopicId)"topic-b", new TestEvent("2"));

        Assert.Equal(2, received.Count);
        Assert.Contains("a:1", received);
        Assert.Contains("b:2", received);
        Assert.DoesNotContain("b:1", received);
        Assert.DoesNotContain("a:2", received);
    }

    [Fact]
    public async Task OnPublish_PublishWithKey_RoutesToPartition()
    {
        await using var bus = CreateBus();
        var partitions = new List<int>();

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { partitions.Add(ctx.Partition.Value); return ValueTask.CompletedTask; });

        for (var i = 0; i < 10; i++)
        {
            await bus.ProduceAsync((TopicId)"test", new TestEvent($"msg-{i}"), $"key-{i}");
        }

        Assert.Equal(10, partitions.Count);
        Assert.All(partitions, p => Assert.InRange(p, 0, 3));
    }
}
