using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Exceptions;
using LiteQueue.Models;
using Xunit;

namespace LiteQueue.Tests;

public record TestMessage(string Value) : MessageBase;

public class PartitionLogTests
{
    private static AppendOnlyLog MakeLog(
        int capacity = 16,
        OverflowPolicy policy = OverflowPolicy.OverwriteOldest)
        => new(new PartitionId(0), (TopicId)"test", capacity, policy);

    private static PartitionLog<TestMessage> Wrap(AppendOnlyLog log)
        => new(log, (TopicId)"test");

    // --- Basic append / read ---

    [Fact]
    public async Task Append_IncrementsOffset()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        var o0 = await pl.AppendAsync(new TestMessage("a"), null, DateTime.UtcNow, default);
        var o1 = await pl.AppendAsync(new TestMessage("b"), null, DateTime.UtcNow, default);

        Assert.Equal(new Offset(0), o0);
        Assert.Equal(new Offset(1), o1);
    }

    [Fact]
    public async Task Read_ReturnsCorrectMessage()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        await pl.AppendAsync(new TestMessage("hello"), null, DateTime.UtcNow, default);
        var msg = pl.Read(new Offset(0));

        Assert.NotNull(msg);
        Assert.Equal("hello", msg.Value.Value);
    }

    [Fact]
    public async Task Read_OutOfRange_ReturnsNull()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        Assert.Null(pl.Read(new Offset(0)));
        await pl.AppendAsync(new TestMessage("x"), null, DateTime.UtcNow, default);
        Assert.Null(pl.Read(new Offset(5)));
    }

    [Fact]
    public async Task ReadRange_ReturnsSlice()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        for (int i = 0; i < 5; i++)
            await pl.AppendAsync(new TestMessage($"m{i}"), null, DateTime.UtcNow, default);

        var slice = pl.ReadRange(new Offset(1), new Offset(4));

        Assert.Equal(3, slice.Count);
        Assert.Equal(new Offset(1), slice[0].Offset);
        Assert.Equal(new Offset(2), slice[1].Offset);
        Assert.Equal(new Offset(3), slice[2].Offset);
    }

    [Fact]
    public async Task ReadRange_FromGeTo_ReturnsEmpty()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        await pl.AppendAsync(new TestMessage("x"), null, DateTime.UtcNow, default);

        var result = pl.ReadRange(new Offset(3), new Offset(1));

        Assert.Empty(result);
    }

    // --- B2: DropNewest must not create offset holes ---

    [Fact]
    public async Task B2_DropNewest_DoesNotIncrementOffset_WhenFull()
    {
        using var log = MakeLog(2, OverflowPolicy.DropNewest);
        var pl = Wrap(log);

        var o0 = await pl.AppendAsync(new TestMessage("a"), null, DateTime.UtcNow, default);
        var o1 = await pl.AppendAsync(new TestMessage("b"), null, DateTime.UtcNow, default);

        // Buffer is full — next append must throw
        await Assert.ThrowsAsync<BufferOverflowException>(
            () => pl.AppendAsync(new TestMessage("c"), null, DateTime.UtcNow, default).AsTask());

        // HeadOffset must not have advanced — no hole
        Assert.Equal(new Offset(2), log.HeadOffset);

        // Existing messages still readable
        Assert.Equal("a", pl.Read(o0)!.Value.Value);
        Assert.Equal("b", pl.Read(o1)!.Value.Value);
    }

    // --- B3: WaitForMessageAsync must return exact requested offset ---

    [Fact]
    public async Task B3_WaitForMessageAsync_ReturnsExactOffset_NotFutureMessage()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        // Append offset 0, 1, 2 — consumer waits for 1
        await pl.AppendAsync(new TestMessage("zero"), null, DateTime.UtcNow, default);
        await pl.AppendAsync(new TestMessage("one"), null, DateTime.UtcNow, default);
        await pl.AppendAsync(new TestMessage("two"), null, DateTime.UtcNow, default);

        var result = await pl.WaitForMessageAsync(new Offset(1), CancellationToken.None);

        Assert.Equal(new Offset(1), result.Offset);
        Assert.Equal("one", result.Value.Value);
    }

    [Fact]
    public async Task B3_WaitForMessageAsync_WaitsAndReceivesOnSignal()
    {
        using var log = MakeLog();
        var pl = Wrap(log);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var waitTask = pl.WaitForMessageAsync(new Offset(0), cts.Token);

        // Not yet written — task should be pending
        Assert.False(waitTask.IsCompleted);

        // Write on a background thread
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            await pl.AppendAsync(new TestMessage("delayed"), null, DateTime.UtcNow, default);
        });

        var result = await waitTask;
        Assert.Equal("delayed", result.Value.Value);
        Assert.Equal(new Offset(0), result.Offset);
    }

    // --- B5: Read during retention does not throw NRE ---

    [Fact]
    public async Task B5_ConcurrentReadAndRetention_DoesNotThrow()
    {
        using var log = MakeLog(64, OverflowPolicy.OverwriteOldest);
        var pl = Wrap(log);

        for (var i = 0; i < 64; i++)
            await pl.AppendAsync(new TestMessage($"msg{i}"), null, DateTime.UtcNow, default);

        var retentionTask = Task.Run(() => pl.ApplyRetention(RetentionPolicy.ByCount.Of(10)));
        var readTasks = Enumerable.Range(0, 20).Select(i =>
            Task.Run(() => pl.Read(new Offset(i)))).ToList();

        await Task.WhenAll([retentionTask, ..readTasks]);
        // If no exception is thrown, B5 is satisfied
    }

    // --- B6: Block mode — consumer advances tail, producer unblocks ---

    [Fact]
    public async Task B6_BlockMode_ProducerUnblocksAfterConsumerAdvancesTail()
    {
        using var log = MakeLog(2, OverflowPolicy.Block);
        var pl = Wrap(log);

        // Fill the buffer
        await pl.AppendAsync(new TestMessage("a"), null, DateTime.UtcNow, default);
        await pl.AppendAsync(new TestMessage("b"), null, DateTime.UtcNow, default);

        using var producerCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var producerTask = pl.AppendAsync(new TestMessage("c"), null, DateTime.UtcNow, producerCts.Token).AsTask();

        Assert.False(producerTask.IsCompleted);

        // Consumer reads — advancing tail frees a slot
        _ = pl.Read(new Offset(0)); // read doesn't advance tail; AdvanceTailTo required
        log.AdvanceTailTo(new Offset(1)); // simulate consumer commit

        var offset = await producerTask;
        Assert.Equal(new Offset(2), offset);
    }

    // --- Retention ---

    [Fact]
    public async Task Retention_ByCount_RemovesOldestMessages()
    {
        using var log = MakeLog(16);
        var pl = Wrap(log);

        for (var i = 0; i < 10; i++)
            await pl.AppendAsync(new TestMessage($"m{i}"), null, DateTime.UtcNow, default);

        var removed = pl.ApplyRetention(RetentionPolicy.ByCount.Of(5));

        Assert.Equal(5, removed);
        Assert.Null(pl.Read(new Offset(0)));
        Assert.Null(pl.Read(new Offset(4)));
        Assert.NotNull(pl.Read(new Offset(5)));
    }

    [Fact]
    public async Task Retention_ByAge_RemovesExpiredMessages()
    {
        using var log = MakeLog(16);
        var pl = Wrap(log);

        var old = DateTime.UtcNow.AddMinutes(-5);
        await log.AppendAsync(new TestMessage("old"), null, old, null, default);
        await pl.AppendAsync(new TestMessage("new"), null, DateTime.UtcNow, default);

        var removed = pl.ApplyRetention(RetentionPolicy.ByAge.Of(TimeSpan.FromMinutes(1)));

        Assert.Equal(1, removed);
        Assert.Null(pl.Read(new Offset(0)));
        Assert.NotNull(pl.Read(new Offset(1)));
    }

    [Fact]
    public async Task Dispose_CompletesChannel()
    {
        using var log = MakeLog();
        var pl = Wrap(log);
        await pl.AppendAsync(new TestMessage("x"), null, DateTime.UtcNow, default);

        log.Dispose();

        Assert.Throws<ObjectDisposedException>(() => pl.Read(new Offset(0)));
    }

    // --- DropNewest rejects when full ---

    [Fact]
    public async Task Append_DropNewest_RejectsWhenFull()
    {
        using var log = MakeLog(3, OverflowPolicy.DropNewest);
        var pl = Wrap(log);

        for (int i = 0; i < 3; i++)
            await pl.AppendAsync(new TestMessage($"m{i}"), null, DateTime.UtcNow, default);

        await Assert.ThrowsAsync<BufferOverflowException>(() =>
            pl.AppendAsync(new TestMessage("overflow"), null, DateTime.UtcNow, default).AsTask());

        Assert.NotNull(pl.Read(new Offset(0)));
        Assert.NotNull(pl.Read(new Offset(1)));
        Assert.NotNull(pl.Read(new Offset(2)));
    }

    // --- OverwriteOldest wraps around ---

    [Fact]
    public async Task Append_OverwriteOldest_OverwritesWhenFull()
    {
        using var log = MakeLog(3);
        var pl = Wrap(log);

        for (int i = 0; i < 5; i++)
            await pl.AppendAsync(new TestMessage($"m{i}"), null, DateTime.UtcNow, default);

        Assert.Null(pl.Read(new Offset(0)));
        Assert.Null(pl.Read(new Offset(1)));
        Assert.NotNull(pl.Read(new Offset(2)));
        Assert.NotNull(pl.Read(new Offset(3)));
        Assert.NotNull(pl.Read(new Offset(4)));
    }

    // --- Block mode cancellation ---

    [Fact]
    public async Task Append_BlockMode_CancellationTokenRespected()
    {
        using var log = MakeLog(2, OverflowPolicy.Block);
        var pl = Wrap(log);

        await pl.AppendAsync(new TestMessage("a"), null, DateTime.UtcNow, default);
        await pl.AppendAsync(new TestMessage("b"), null, DateTime.UtcNow, default);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            pl.AppendAsync(new TestMessage("c"), null, DateTime.UtcNow, cts.Token).AsTask());
    }
}
