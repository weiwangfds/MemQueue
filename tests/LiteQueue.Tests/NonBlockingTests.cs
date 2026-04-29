using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation;
using LiteQueue.Implementation.PartitionSelectors;
using LiteQueue.Implementation.RebalanceStrategies;
using LiteQueue.Internal;
using LiteQueue.Models;
using Xunit;

namespace LiteQueue.Tests;

public class NonBlockingTests
{
    // --- C1: AppendOnlyLog RWLock TryEnter ---

    [Fact]
    public async Task AppendOnlyLog_ConcurrentReadAndRetention_NoTimeoutException()
    {
        using var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test", 128, OverflowPolicy.OverwriteOldest);

        for (var i = 0; i < 64; i++)
            await log.AppendAsync(new TestMessage($"msg{i}"), null, DateTime.UtcNow, null, default);

        var tasks = new List<Task>();
        for (var r = 0; r < 10; r++)
        {
            tasks.Add(Task.Run(() => log.ApplyRetention(RetentionPolicy.ByCount.Of(10))));
            for (var i = 0; i < 20; i++)
            {
                var offset = new Offset(i);
                tasks.Add(Task.Run(() => log.ReadRaw(offset)));
            }
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task AppendOnlyLog_AdvanceTail_ConcurrentWriters_NoHang()
    {
        using var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test", 100, OverflowPolicy.OverwriteOldest);

        var writers = Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            for (var i = 0; i < 100; i++)
                await log.AppendAsync(new TestMessage($"msg"), null, DateTime.UtcNow, null, default);
        })).ToList();

        var advancers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 50; i++)
                log.AdvanceTailTo(new Offset(i * 4));
        })).ToList();

        await Task.WhenAll([..writers, ..advancers]);
    }

    // --- C1: ConsumerGroup RWLock TryEnter ---

    [Fact]
    public async Task ConsumerGroup_ConcurrentRegisterAndQuery_NoTimeoutException()
    {
        var strategy = new RangeRebalancer();
        using var group = new ConsumerGroup((TopicId)"test", strategy);

        var tasks = new List<Task>();
        for (var i = 0; i < 20; i++)
        {
            var consumerId = new ConsumerId($"consumer-{i}");
            tasks.Add(Task.Run(() => group.Register(consumerId, 8)));
        }

        for (var i = 0; i < 10; i++)
        {
            var consumerId = new ConsumerId($"consumer-{i % 5}");
            tasks.Add(Task.Run(() => group.GetAssigned(consumerId)));
        }

        for (var i = 0; i < 5; i++)
        {
            tasks.Add(Task.Run(() => group.CommitOffset(new PartitionId(i), new Offset(i * 10))));
            tasks.Add(Task.Run(() => group.GetCommittedOffset(new PartitionId(i))));
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public void ConsumerGroup_Dispose_DoesNotHang_WhenUnderContention()
    {
        var strategy = new RangeRebalancer();

        for (var run = 0; run < 20; run++)
        {
            using var group = new ConsumerGroup((TopicId)"test", strategy);
            Parallel.For(0, 10, i =>
            {
                group.Register(new ConsumerId($"c-{i}"), 4);
                group.GetAssigned(new ConsumerId($"c-{i}"));
                group.CommitOffset(new PartitionId(0), new Offset(i));
            });
        }
    }

    // --- C2: SubscriptionManager DisposeAsync timeout ---

    [Fact]
    public async Task SubscriptionManager_DisposeAsync_DoesNotHang_WhenHandlerStuck()
    {
        var manager = new SubscriptionManager();
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());
        var consumer = bus.CreateConsumer<TestMessage>((TopicId)"test");

        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var startTask = manager.StartAsync<TestMessage>(
            consumer,
            async (msg, ctx, ct) =>
            {
                handlerStarted.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
            },
            autoCommit: false,
            cts.Token);

        await bus.ProduceAsync((TopicId)"test", new TestMessage("trigger"));
        await handlerStarted.Task;

        cts.Cancel();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await manager.DisposeAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(15),
            $"DisposeAsync took {sw.Elapsed.TotalSeconds:F1}s — should have timed out within 10s");

        await bus.DisposeAsync();
    }

    // --- C3: RetentionManager CT propagation ---

    [Fact]
    public async Task RetentionManager_StopAsync_RespectsExternalCancellationToken()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test", o =>
        {
            o.PartitionCount = 1;
            o.Retention = RetentionPolicy.ByCount.Of(5);
        });

        var rm = new RetentionManager(tm, TimeSpan.FromMilliseconds(50));
        await rm.StartAsync();

        for (var i = 0; i < 10; i++)
            await tm.GetPartitionStore<TestMessage>((TopicId)"test", new PartitionId(0))!
                .AppendAsync(new TestMessage($"m{i}"), null, DateTime.UtcNow, default);

        await Task.Delay(100);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await rm.StopAsync(cts.Token);
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"StopAsync took {sw.Elapsed.TotalSeconds:F1}s — CT not propagated");

        await rm.DisposeAsync();
    }

    [Fact]
    public async Task RetentionManager_StartAsync_PropagatesExternalCancellationToken()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test", o =>
        {
            o.Retention = RetentionPolicy.ByCount.Of(5);
        });

        using var externalCts = new CancellationTokenSource();
        var rm = new RetentionManager(tm, TimeSpan.FromMilliseconds(50));
        await rm.StartAsync(externalCts.Token);

        await Task.Delay(100);
        await externalCts.CancelAsync();

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await rm.DisposeAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3),
            $"DisposeAsync took {sw.Elapsed.TotalSeconds:F1}s — external CT not linked to loop");

        await rm.DisposeAsync();
    }

    // --- C4+H2: EventBus callback isolation ---

    [Fact]
    public async Task EventBus_FireAsync_HandlerException_DoesNotBlockOtherHandlers()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        var handler2Called = false;
        var handler3Called = false;

        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => throw new InvalidOperationException("boom"));
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { handler2Called = true; return ValueTask.CompletedTask; });
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { handler3Called = true; return ValueTask.CompletedTask; });

        var result = await bus.ProduceAsync((TopicId)"test", new TestEvent("isolation-test"));

        Assert.True(handler2Called, "Second handler should be invoked even if first throws");
        Assert.True(handler3Called, "Third handler should be invoked even if first throws");
        Assert.True(result.Offset.IsValid);
    }

    [Fact]
    public async Task EventBus_FireAsync_AsyncHandlerFailure_DoesNotBlockOtherHandlers()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"test");
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        var handler2Called = false;

        bus.OnPublish<TestEvent>((TopicId)"test", async (msg, ctx, ct) =>
        {
            await Task.Delay(10, ct);
            throw new InvalidOperationException("async boom");
        });
        bus.OnPublish<TestEvent>((TopicId)"test", (msg, ctx, ct) => { handler2Called = true; return ValueTask.CompletedTask; });

        var result = await bus.ProduceAsync((TopicId)"test", new TestEvent("async-isolation"));

        Assert.True(handler2Called, "Second handler should be invoked even if async first handler throws");
        Assert.True(result.Offset.IsValid);
    }

    // --- C4: BackpressureCoordinator event isolation ---

    [Fact]
    public async Task BackpressureCoordinator_UserEventHandlerFailure_DoesNotCrashLibrary()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test", 5, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test", new BackpressureOptions
        {
            HighWatermarkPercent = 0.6,
            LowWatermarkPercent = 0.3
        });

        bp.HighWatermarkReached += (_, _, _) => throw new InvalidOperationException("user handler crash");
        bp.LowWatermarkRecovered += (_, _, _) => throw new InvalidOperationException("user handler crash 2");

        for (var i = 0; i < 4; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        var count = log.HeadOffset.Value - log.TailOffset.Value;
        var utilization = 5 > 0 ? (double)count / 5 : 0;
        bp.UpdateStats((TopicId)"test", new PartitionId(0), utilization, log.HeadOffset);

        var ex = Record.Exception(() => bp.CheckWatermark((TopicId)"test", new PartitionId(0), utilization));
        Assert.Null(ex);

        bp.OnOffsetCommitted((TopicId)"test", (ConsumerGroupId)"g1", new PartitionId(0), new Offset(3));
        var count2 = log.HeadOffset.Value - log.TailOffset.Value;
        var utilization2 = 5 > 0 ? (double)count2 / 5 : 0;
        bp.UpdateStats((TopicId)"test", new PartitionId(0), utilization2, log.HeadOffset);

        var ex2 = Record.Exception(() => bp.CheckWatermark((TopicId)"test", new PartitionId(0), utilization2));
        Assert.Null(ex2);

        bp.Dispose();
    }

    [Fact]
    public async Task BackpressureCoordinator_SlowConsumerEventHandlerFailure_DoesNotCrash()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test", 20, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test", new BackpressureOptions
        {
            SlowConsumerStrategy = SlowConsumerStrategy.Warn,
            SlowConsumerLagThreshold = 3
        });

        bp.SlowConsumerDetected += (_, _, _, _) => throw new InvalidOperationException("user crash");

        for (var i = 0; i < 10; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        var count = log.HeadOffset.Value - log.TailOffset.Value;
        var utilization = 20 > 0 ? (double)count / 20 : 0;
        bp.UpdateStats((TopicId)"test", new PartitionId(0), utilization, log.HeadOffset);

        var ex = Record.Exception(() =>
            bp.OnOffsetCommitted((TopicId)"test", (ConsumerGroupId)"slow", new PartitionId(0), new Offset(2)));
        Assert.Null(ex);

        bp.Dispose();
    }

    // --- H3: InMemoryDeadLetterQueue snapshot ---

    [Fact]
    public void DeadLetterQueue_GetErrors_ReturnsSnapshot()
    {
        var dlq = new InMemoryDeadLetterQueue(100);

        for (var i = 0; i < 10; i++)
            dlq.Publish(new DeliveryError
            {
                Topic = "test", Partition = 0, Offset = i,
                ErrorType = "TestError", ErrorMessage = $"err{i}"
            }, new TestMessage($"msg{i}"));

        var snapshot = dlq.GetErrors("test");
        Assert.Equal(10, snapshot.Count);

        dlq.Publish(new DeliveryError
        {
            Topic = "test", Partition = 0, Offset = 99,
            ErrorType = "TestError", ErrorMessage = "extra"
        }, new TestMessage("extra"));

        Assert.Equal(10, snapshot.Count);
        Assert.Equal(11, dlq.GetErrors("test").Count);
    }

    [Fact]
    public void DeadLetterQueue_ConcurrentPublishAndGetErrors_NoException()
    {
        var dlq = new InMemoryDeadLetterQueue(200);

        var tasks = new List<Task>();
        for (var i = 0; i < 50; i++)
        {
            var idx = i;
            tasks.Add(Task.Run(() => dlq.Publish(
                new DeliveryError
                {
                    Topic = "test", Partition = 0, Offset = idx,
                    ErrorType = "TestError", ErrorMessage = $"err{idx}"
                }, new TestMessage($"msg{idx}"))));
            tasks.Add(Task.Run(() => dlq.GetErrors("test")));
        }

        var ex = Record.Exception(() => Task.WaitAll(tasks.ToArray()));
        Assert.Null(ex);

        var errors = dlq.GetErrors("test");
        Assert.True(errors.Count > 0);
    }

    // --- H5: Bus EnsureTopicExists async path ---

    [Fact]
    public async Task Bus_ProduceAsync_AutoCreatesTopic_WithoutBlocking()
    {
        using var tm = new TopicManager();
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        var sw = System.Diagnostics.Stopwatch.StartNew();
        var result = await bus.ProduceAsync((TopicId)"auto-topic", new TestMessage("hello"));
        sw.Stop();

        Assert.True(result.Offset.IsValid);
        Assert.True(tm.TopicExists((TopicId)"auto-topic"));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task Bus_SubscribeAsync_AutoCreatesTopic_WithoutBlocking()
    {
        using var tm = new TopicManager();
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var subscribeTask = bus.SubscribeAsync<TestMessage>(
            (TopicId)"sub-topic",
            (msg, ctx, ct) => ValueTask.CompletedTask,
            cts.Token);

        Assert.True(tm.TopicExists((TopicId)"sub-topic"));

        cts.Cancel();
        try { await subscribeTask; } catch (OperationCanceledException) { }
    }

    [Fact]
    public async Task Bus_MultipleConcurrentProduceAsync_NewTopic_NoException()
    {
        using var tm = new TopicManager();
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        var tasks = Enumerable.Range(0, 20).Select(i =>
            bus.ProduceAsync((TopicId)"concurrent-topic", new TestMessage($"msg{i}")).AsTask()).ToList();

        await Task.WhenAll(tasks);

        Assert.True(tm.TopicExists((TopicId)"concurrent-topic"));
        Assert.All(tasks, t => Assert.True(t.IsCompletedSuccessfully));
    }

    // --- Integration: full pipeline non-blocking ---

    [Fact]
    public async Task FullPipeline_ProduceAndConsume_DoesNotBlock()
    {
        using var tm = new TopicManager();
        tm.CreateTopic((TopicId)"pipeline", o => { o.PartitionCount = 2; });
        await using var bus = new Bus(tm, new GroupCoordinator(), new RoundRobinPartitioner(), new RangeRebalancer());

        var received = new List<string>();
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var consumeTask = bus.SubscribeAsync<TestMessage>(
            (TopicId)"pipeline",
            (msg, ctx, ct) =>
            {
                lock (received)
                {
                    received.Add(msg.Value);
                    if (received.Count >= 10) allReceived.SetResult();
                }
                return ValueTask.CompletedTask;
            },
            cts.Token);

        await Task.Delay(100);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        for (var i = 0; i < 10; i++)
            await bus.ProduceAsync((TopicId)"pipeline", new TestMessage($"msg-{i}"));
        sw.Stop();

        await allReceived.Task;
        cts.Cancel();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1), $"Producing 10 messages took {sw.Elapsed.TotalSeconds:F2}s");
        Assert.Equal(10, received.Count);

        try { await consumeTask; } catch (OperationCanceledException) { }
    }
}
