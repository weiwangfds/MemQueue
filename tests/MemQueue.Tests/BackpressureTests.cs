using MemQueue.Core;
using MemQueue.Models;
using Xunit;

namespace MemQueue.Tests;

public class BackpressureTests
{
    private static void UpdateBpStats(BackpressureCoordinator bp, AppendOnlyLog log,
        TopicId topic, PartitionId partition, int capacity)
    {
        var count = log.HeadOffset.Value - log.TailOffset.Value;
        var utilization = capacity > 0 ? (double)count / capacity : 0;
        bp.UpdateStats(topic, partition, utilization, log.HeadOffset);
        bp.CheckWatermark(topic, partition, utilization);
    }

    [Fact]
    public async Task BlockMode_ConsumerCommit_FreesSpace()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 3, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions());

        for (var i = 0; i < 3; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var appendTask = log.AppendAsync(new TestMessage("new"), null, DateTime.UtcNow, null, cts.Token).AsTask();

        await Task.Delay(50);
        Assert.False(appendTask.IsCompleted);

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-1", new PartitionId(0), new Offset(2));

        var result = await appendTask;
        Assert.True(result.Value >= 3);

        bp.Dispose();
    }

    [Fact]
    public async Task BlockMode_MultipleGroups_MinWatermark()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 3, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions());

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-a", new PartitionId(0), new Offset(-1));
        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-b", new PartitionId(0), new Offset(-1));

        for (var i = 0; i < 3; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-a", new PartitionId(0), new Offset(2));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            log.AppendAsync(new TestMessage("blocked"), null, DateTime.UtcNow, null, cts.Token).AsTask());

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-b", new PartitionId(0), new Offset(2));

        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var result = await log.AppendAsync(new TestMessage("unblocked"), null, DateTime.UtcNow, null, cts2.Token);
        Assert.True(result.Value >= 3);

        bp.Dispose();
    }

    [Fact]
    public async Task HighWatermark_EventFired()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 5, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions { HighWatermarkPercent = 0.6 });

        string? firedTopic = null;
        int firedPartition = -1;
        double firedUtilization = -1;
        bp.HighWatermarkReached += (t, p, u) => { firedTopic = t; firedPartition = p; firedUtilization = u; };

        for (var i = 0; i < 4; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 5);

        Assert.Equal("test-topic", firedTopic);
        Assert.Equal(0, firedPartition);
        Assert.True(firedUtilization >= 0.6);

        bp.Dispose();
    }

    [Fact]
    public async Task SlowConsumer_WarnEvent()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 20, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions
        {
            SlowConsumerStrategy = SlowConsumerStrategy.Warn,
            SlowConsumerLagThreshold = 5
        });

        for (var i = 0; i < 10; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 20);

        string? slowGroup = null;
        long detectedLag = -1;
        bp.SlowConsumerDetected += (t, p, g, l) => { slowGroup = g; detectedLag = l; };

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"slow-group", new PartitionId(0), new Offset(2));

        Assert.Equal("slow-group", slowGroup);
        Assert.True(detectedLag > 5);

        bp.Dispose();
    }

    [Fact]
    public async Task LowWatermark_RecoveryEvent()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 5, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions
        {
            HighWatermarkPercent = 0.6,
            LowWatermarkPercent = 0.3
        });

        bool recovered = false;
        bp.LowWatermarkRecovered += (t, p, u) => recovered = true;

        for (var i = 0; i < 4; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 5);

        Assert.False(recovered);

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-1", new PartitionId(0), new Offset(3));

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 5);

        Assert.True(recovered);

        bp.Dispose();
    }

    [Fact]
    public async Task GetUtilization_ReturnsCorrectValue()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 10, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);

        Assert.Equal(0.0, bp.GetUtilization("test-topic", 0));

        for (var i = 0; i < 5; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 10);

        var utilization = bp.GetUtilization("test-topic", 0);
        Assert.Equal(0.5, utilization, 1);

        bp.Dispose();
    }

    [Fact]
    public async Task GetConsumerLag_TracksCorrectly()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 10, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);

        Assert.Equal(-1, bp.GetConsumerLag("test-topic", 0));

        for (var i = 0; i < 5; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        UpdateBpStats(bp, log, (TopicId)"test-topic", new PartitionId(0), 10);
        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-1", new PartitionId(0), new Offset(2));

        var lag = bp.GetConsumerLag("test-topic", 0);
        Assert.Equal(3, lag);

        bp.Dispose();
    }

    [Fact]
    public async Task OnGroupRemoved_RecomputesWatermark()
    {
        var bp = new BackpressureCoordinator();
        var log = new AppendOnlyLog(new PartitionId(0), (TopicId)"test-topic", 3, OverflowPolicy.Block);
        bp.RegisterPartition((TopicId)"test-topic", new PartitionId(0), log);
        bp.SetOptions((TopicId)"test-topic", new BackpressureOptions());

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-a", new PartitionId(0), new Offset(-1));
        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-b", new PartitionId(0), new Offset(-1));

        for (var i = 0; i < 3; i++)
            await log.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, null, default);

        bp.OnOffsetCommitted((TopicId)"test-topic", (ConsumerGroupId)"group-a", new PartitionId(0), new Offset(2));

        using var cts1 = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            log.AppendAsync(new TestMessage("blocked"), null, DateTime.UtcNow, null, cts1.Token).AsTask());

        bp.OnGroupRemoved((TopicId)"test-topic", (ConsumerGroupId)"group-b", 1);

        using var cts2 = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var result = await log.AppendAsync(new TestMessage("unblocked"), null, DateTime.UtcNow, null, cts2.Token);
        Assert.True(result.Value >= 3);

        bp.Dispose();
    }
}
