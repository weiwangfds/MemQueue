// src/MemQueue/Core/GroupCoordinator.cs
using System.Collections.Concurrent;
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Core;

public sealed class GroupCoordinator : IDisposable
{
    private readonly ConcurrentDictionary<ConsumerGroupId, ConsumerGroup> _groups = new();
    private BackpressureCoordinator? _backpressureCoordinator;

    internal void SetBackpressureCoordinator(BackpressureCoordinator coordinator)
        => _backpressureCoordinator = coordinator;

    public IReadOnlyList<PartitionId> RegisterConsumer(
        ConsumerGroupId groupId, TopicId topic, ConsumerId consumerId,
        IRebalancer strategy, int partitionCount)
    {
        var group = _groups.GetOrAdd(groupId, _ => new ConsumerGroup(topic, strategy));
        return group.Register(consumerId, partitionCount);
    }

    public (IReadOnlyList<PartitionId> Assigned, IReadOnlyList<PartitionId> Revoked)
        UnregisterConsumer(ConsumerGroupId groupId, ConsumerId consumerId, int partitionCount)
    {
        if (!_groups.TryGetValue(groupId, out var group))
            return ([], []);

        var result = group.Unregister(consumerId, partitionCount);
        _backpressureCoordinator?.OnGroupRemoved(group.Topic, groupId, partitionCount);
        return result;
    }

    public IReadOnlyList<PartitionId> GetAssignedPartitions(ConsumerGroupId groupId, ConsumerId consumerId)
        => _groups.TryGetValue(groupId, out var group) ? group.GetAssigned(consumerId) : [];

    public void CommitOffset(ConsumerGroupId groupId, PartitionId partition, Offset offset)
    {
        if (!_groups.TryGetValue(groupId, out var group)) return;
        group.CommitOffset(partition, offset);
        _backpressureCoordinator?.OnOffsetCommitted(group.Topic, groupId, partition, offset);
    }

    public Offset GetCommittedOffset(ConsumerGroupId groupId, PartitionId partition)
        => _groups.TryGetValue(groupId, out var group)
            ? group.GetCommittedOffset(partition)
            : Offset.Unset;

    public void Dispose()
    {
        foreach (var g in _groups.Values) g.Dispose();
        _groups.Clear();
    }
}
