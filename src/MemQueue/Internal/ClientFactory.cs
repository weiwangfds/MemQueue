// src/MemQueue/Internal/ClientFactory.cs
using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Models;

namespace MemQueue.Internal;

internal sealed class ClientFactory
{
    private readonly ITopicManager _topicManager;
    private readonly GroupCoordinator _coordinator;
    private readonly IPartitioner _partitionSelector;
    private readonly IRebalancer _rebalanceStrategy;

    public ClientFactory(
        ITopicManager topicManager,
        GroupCoordinator coordinator,
        IPartitioner partitionSelector,
        IRebalancer rebalanceStrategy)
    {
        _topicManager = topicManager;
        _coordinator = coordinator;
        _partitionSelector = partitionSelector;
        _rebalanceStrategy = rebalanceStrategy;
    }

    public IProducer<TMessage> CreateProducer<TMessage>(TopicId topic) where TMessage : MessageBase
        => new Producer<TMessage>(topic, _topicManager, _partitionSelector);

    public Consumer<TMessage> CreateGroupConsumer<TMessage>(TopicId topic, ConsumerGroupId groupId)
        where TMessage : MessageBase
    {
        var consumerId = new ConsumerId(Guid.NewGuid().ToString("N")[..16]);
        var partitionCount = _topicManager.GetTopicOptions(topic).PartitionCount;
        _coordinator.RegisterConsumer(groupId, topic, consumerId, _rebalanceStrategy, partitionCount);
        return new Consumer<TMessage>(topic, groupId, _topicManager, _coordinator, consumerId);
    }

    public IConsumer<TMessage> CreateConsumer<TMessage>(TopicId topic, ConsumerGroupId? groupId = null)
        where TMessage : MessageBase
    {
        if (groupId is not null)
        {
            var consumerId = new ConsumerId(Guid.NewGuid().ToString("N")[..16]);
            var partitionCount = _topicManager.GetTopicOptions(topic).PartitionCount;
            _coordinator.RegisterConsumer(groupId.Value, topic, consumerId, _rebalanceStrategy, partitionCount);
            return new Consumer<TMessage>(topic, groupId, _topicManager, _coordinator, consumerId);
        }
        return new Consumer<TMessage>(topic, null, _topicManager, null);
    }
}
