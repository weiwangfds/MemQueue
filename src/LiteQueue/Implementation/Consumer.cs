// src/LiteQueue/Implementation/Consumer.cs
using System.Collections.Concurrent;
using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Models;

namespace LiteQueue.Implementation;

public sealed class Consumer<TMessage> : IConsumer<TMessage> where TMessage : MessageBase
{
    private readonly ITopicManager _topicManager;
    private readonly GroupCoordinator? _coordinator;
    private readonly ConsumerId _consumerId;
    private readonly ConcurrentDictionary<PartitionId, Offset> _readOffsets = new();
    private volatile IReadOnlyList<PartitionId>? _cachedPartitions;
    private bool _disposed;

    public TopicId Topic { get; }
    public ConsumerGroupId? GroupId { get; }
    internal ConsumerId ConsumerId => _consumerId;

    public Consumer(
        TopicId topic,
        ConsumerGroupId? groupId,
        ITopicManager topicManager,
        GroupCoordinator? coordinator,
        ConsumerId? consumerId = null)
    {
        Topic = topic;
        GroupId = groupId;
        _topicManager = topicManager;
        _coordinator = coordinator;
        _consumerId = consumerId ?? new ConsumerId(Guid.NewGuid().ToString("N")[..16]);
    }

    public ValueTask<ConsumeResult<TMessage>> ConsumeAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ConsumeCoreAsync(ct);
    }

    private async ValueTask<ConsumeResult<TMessage>> ConsumeCoreAsync(CancellationToken ct)
    {
        // Wait for partition assignment (group consumers start with empty list)
        IReadOnlyList<PartitionId> partitions;
        while (true)
        {
            partitions = GetAssignedPartitions();
            if (partitions.Count > 0) break;
            await Task.Delay(50, ct);
        }

        // Fast path: any partition already has the next message buffered
        foreach (var partition in partitions)
        {
            var offset = GetReadOffset(partition);
            var store = _topicManager.GetPartitionStore<TMessage>(Topic, partition);
            if (store is null) continue;
            if (offset < store.HeadOffset)
            {
                var envelope = store.Read(offset);
                if (envelope is not null)
                {
                    SetReadOffset(partition, offset.Next());
                    return BuildResult(envelope);
                }
            }
        }

        // P1 fix: wait on ALL partitions concurrently — respond to whichever fires first
        return await WaitAnyPartitionAsync(partitions, ct);
    }

    // P1: Task.WhenAny across all assigned partitions
    private async ValueTask<ConsumeResult<TMessage>> WaitAnyPartitionAsync(
        IReadOnlyList<PartitionId> partitions, CancellationToken ct)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var tasks = partitions
            .Select(p =>
            {
                var offset = GetReadOffset(p);
                var store = _topicManager.GetPartitionStore<TMessage>(Topic, p);
                if (store is null) return Task.FromCanceled<MessageEnvelope<TMessage>>(linkedCts.Token);
                return store.WaitForMessageAsync(offset, linkedCts.Token).AsTask();
            })
            .ToList();

        // B1 fix: exceptions propagate — no empty catch. ObjectDisposedException → caller handles.
        var winner = await Task.WhenAny(tasks);

        // Cancel remaining partition waiters cleanly
        await linkedCts.CancelAsync();

        var envelope = await winner; // re-await to propagate any exception
        SetReadOffset(envelope.Partition, GetReadOffset(envelope.Partition).Next());
        return BuildResult(envelope);
    }

    public async IAsyncEnumerable<ConsumeResult<TMessage>> ConsumeAllAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        while (!ct.IsCancellationRequested && !_disposed)
        {
            ConsumeResult<TMessage> result;
            try
            {
                result = await ConsumeCoreAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested || _disposed)
            {
                yield break;
            }
            // B1 fix: ObjectDisposedException (partition deleted) exits the loop cleanly
            catch (ObjectDisposedException)
            {
                yield break;
            }

            yield return result;
        }
    }

    public ValueTask CommitAsync(Offset offset, PartitionId partition, CancellationToken ct = default)
    {
        if (_coordinator is null || GroupId is null) return ValueTask.CompletedTask;
        _coordinator.CommitOffset(GroupId.Value, partition, offset);
        return ValueTask.CompletedTask;
    }

    public ValueTask CommitAsync(CancellationToken ct = default)
    {
        if (_coordinator is null || GroupId is null) return ValueTask.CompletedTask;
        foreach (var partition in GetAssignedPartitions())
        {
            if (_readOffsets.TryGetValue(partition, out var o) && o.IsValid)
                _coordinator.CommitOffset(GroupId.Value, partition, new Offset(o.Value - 1));
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask SeekAsync(PartitionId partition, Offset offset, CancellationToken ct = default)
    {
        _readOffsets[partition] = offset;
        return ValueTask.CompletedTask;
    }

    public ValueTask SeekToBeginningAsync(PartitionId partition, CancellationToken ct = default)
    {
        var store = _topicManager.GetPartitionStore(Topic, partition)
            ?? throw new InvalidOperationException($"Partition {partition} does not exist for topic '{Topic}'.");
        _readOffsets[partition] = store.TailOffset;
        return ValueTask.CompletedTask;
    }

    public ValueTask SeekToEndAsync(PartitionId partition, CancellationToken ct = default)
    {
        var store = _topicManager.GetPartitionStore(Topic, partition)
            ?? throw new InvalidOperationException($"Partition {partition} does not exist for topic '{Topic}'.");
        _readOffsets[partition] = store.HeadOffset;
        return ValueTask.CompletedTask;
    }

    public long GetPosition(PartitionId partition)
        => _readOffsets.TryGetValue(partition, out var o) ? o.Value : -1;

    // B8 fix: called after rebalance — clears stale offsets so next read starts from committed
    internal void OnPartitionsRevoked(IReadOnlyList<PartitionId> revoked)
    {
        foreach (var p in revoked) _readOffsets.TryRemove(p, out _);
    }

    private IReadOnlyList<PartitionId> GetAssignedPartitions()
    {
        if (GroupId is not null && _coordinator is not null)
            return _coordinator.GetAssignedPartitions(GroupId.Value, _consumerId);

        if (_cachedPartitions is not null) return _cachedPartitions;

        var stores = _topicManager.GetAllPartitionStores(Topic);
        var partitions = stores.Select(s => s.PartitionId).ToArray();
        _cachedPartitions = partitions;
        return partitions;
    }

    private Offset GetReadOffset(PartitionId partition)
    {
        if (_readOffsets.TryGetValue(partition, out var offset)) return offset;

        Offset startOffset;
        if (GroupId is not null && _coordinator is not null)
        {
            var committed = _coordinator.GetCommittedOffset(GroupId.Value, partition);
            startOffset = committed.IsValid ? committed : GetTailOffset(partition);
        }
        else
        {
            startOffset = GetTailOffset(partition);
        }

        _readOffsets[partition] = startOffset;
        return startOffset;
    }

    private Offset GetTailOffset(PartitionId partition)
    {
        var store = _topicManager.GetPartitionStore(Topic, partition);
        return store?.TailOffset ?? Offset.Unset;
    }

    private void SetReadOffset(PartitionId partition, Offset offset)
        => _readOffsets[partition] = offset;

    private static ConsumeResult<TMessage> BuildResult(MessageEnvelope<TMessage> envelope) => new()
    {
        Topic = envelope.Topic,
        Partition = envelope.Partition,
        Offset = envelope.Offset,
        Key = envelope.Key,
        Value = envelope.Value,
        Timestamp = envelope.Timestamp,
        Headers = envelope.Headers
    };

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
