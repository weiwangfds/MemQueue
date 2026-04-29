using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Models;

namespace MemQueue.Implementation;

public sealed class ConsumerGroup<TMessage> : IAsyncDisposable
    where TMessage : MessageBase
{
    private readonly GroupCoordinator _coordinator;
    private readonly Consumer<TMessage> _consumer;
    private readonly ConsumerId _consumerId;
    private readonly IRebalancer _strategy;
    private readonly int _partitionCount;
    private bool _disposed;

    public ConsumerGroupId GroupId { get; }
    public TopicId Topic { get; }

    public ConsumerGroup(
        ConsumerGroupId groupId,
        TopicId topic,
        ITopicManager topicManager,
        GroupCoordinator coordinator,
        IRebalancer strategy)
    {
        GroupId = groupId;
        Topic = topic;
        _coordinator = coordinator;
        _strategy = strategy;
        _consumerId = new ConsumerId(Guid.NewGuid().ToString("N")[..16]);

        var options = topicManager.GetTopicOptions(topic);
        _partitionCount = options.PartitionCount;

        _coordinator.RegisterConsumer(groupId, topic, _consumerId, _strategy, _partitionCount);
        _consumer = new Consumer<TMessage>(topic, groupId, topicManager, coordinator, _consumerId);
    }

    public IAsyncEnumerable<ConsumeResult<TMessage>> ConsumeAllAsync(
        CancellationToken cancellationToken = default)
    {
        return _consumer.ConsumeAllAsync(cancellationToken);
    }

    public ValueTask<ConsumeResult<TMessage>> ConsumeAsync(
        CancellationToken cancellationToken = default)
    {
        return _consumer.ConsumeAsync(cancellationToken);
    }

    public ValueTask CommitAsync(Offset offset, PartitionId partition, CancellationToken cancellationToken = default)
    {
        return _consumer.CommitAsync(offset, partition, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        await _consumer.DisposeAsync();
        _coordinator.UnregisterConsumer(GroupId, _consumerId, _partitionCount);
    }
}
