// src/LiteQueue/Core/ConsumerGroup.cs
using LiteQueue.Abstractions;
using LiteQueue.Models;

namespace LiteQueue.Core;

internal sealed class ConsumerGroup : IDisposable
{
    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(5);

    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private readonly List<ConsumerId> _members = new();
    private readonly Dictionary<ConsumerId, IReadOnlyList<PartitionId>> _assignments = new();
    private readonly Dictionary<PartitionId, Offset> _committedOffsets = new();
    private readonly IRebalancer _strategy;

    public TopicId Topic { get; }

    public ConsumerGroup(TopicId topic, IRebalancer strategy)
    {
        Topic = topic;
        _strategy = strategy;
    }

    // B4 fix: read + compute + write in single write lock — no double-window race
    public IReadOnlyList<PartitionId> Register(ConsumerId consumerId, int partitionCount)
    {
        if (!_lock.TryEnterWriteLock(LockTimeout))
            throw new TimeoutException($"ConsumerGroup.Register lock timeout for topic {Topic}.");
        try
        {
            if (_members.Contains(consumerId))
                return _assignments.TryGetValue(consumerId, out var ex) ? ex : [];

            _members.Add(consumerId);
            var raw = _strategy.Assign(partitionCount, _members.Select(m => (string)m).ToList());
            _assignments.Clear();
            foreach (var kvp in raw)
                _assignments[new ConsumerId(kvp.Key)] = kvp.Value.Select(p => new PartitionId(p)).ToList();

            return _assignments.TryGetValue(consumerId, out var assigned) ? assigned : [];
        }
        finally { _lock.ExitWriteLock(); }
    }

    // B8 fix: returns revoked partitions so Consumer can clear its _readOffsets
    public (IReadOnlyList<PartitionId> Assigned, IReadOnlyList<PartitionId> Revoked)
        Unregister(ConsumerId consumerId, int partitionCount)
    {
        if (!_lock.TryEnterWriteLock(LockTimeout))
            throw new TimeoutException($"ConsumerGroup.Unregister lock timeout for topic {Topic}.");
        try
        {
            var previous = _assignments.TryGetValue(consumerId, out var prev) ? prev : [];
            _members.Remove(consumerId);
            _assignments.Remove(consumerId);
            _committedOffsets.Clear(); // cleared on group membership change

            if (_members.Count == 0)
                return ([], previous);

            var raw = _strategy.Assign(partitionCount, _members.Select(m => (string)m).ToList());
            _assignments.Clear();
            foreach (var kvp in raw)
                _assignments[new ConsumerId(kvp.Key)] = kvp.Value.Select(p => new PartitionId(p)).ToList();

            return ([], previous); // returning revoked so Consumer can clear offsets
        }
        finally { _lock.ExitWriteLock(); }
    }

    public IReadOnlyList<PartitionId> GetAssigned(ConsumerId consumerId)
    {
        if (!_lock.TryEnterReadLock(LockTimeout))
            throw new TimeoutException($"ConsumerGroup.GetAssigned lock timeout for topic {Topic}.");
        try
        {
            return _assignments.TryGetValue(consumerId, out var p) ? p : [];
        }
        finally { _lock.ExitReadLock(); }
    }

    public void CommitOffset(PartitionId partition, Offset offset)
    {
        if (!_lock.TryEnterWriteLock(LockTimeout))
            throw new TimeoutException($"ConsumerGroup.CommitOffset lock timeout for topic {Topic}.");
        try { _committedOffsets[partition] = offset; }
        finally { _lock.ExitWriteLock(); }
    }

    public Offset GetCommittedOffset(PartitionId partition)
    {
        if (!_lock.TryEnterReadLock(LockTimeout))
            throw new TimeoutException($"ConsumerGroup.GetCommittedOffset lock timeout for topic {Topic}.");
        try
        {
            return _committedOffsets.TryGetValue(partition, out var o) ? o : Offset.Unset;
        }
        finally { _lock.ExitReadLock(); }
    }

    public void Dispose()
    {
        if (!_lock.TryEnterWriteLock(LockTimeout))
        {
            _lock.Dispose();
            return;
        }
        try { _members.Clear(); _assignments.Clear(); _committedOffsets.Clear(); }
        finally { _lock.ExitWriteLock(); _lock.Dispose(); }
    }
}
