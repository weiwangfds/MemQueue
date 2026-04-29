// src/LiteQueue/Implementation/Bus.cs
using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Implementation.PartitionSelectors;
using LiteQueue.Internal;
using LiteQueue.Models;

namespace LiteQueue.Implementation;

public sealed class Bus : IMessageBus, IDomainEventBus, IAsyncDisposable
{
    private readonly ITopicManager _topicManager;
    private readonly GroupCoordinator _coordinator;
    private readonly IPartitioner _partitionSelector;
    private readonly KeyHashPartitioner _keyHashPartitioner = new();
    private readonly EventBus _eventBus = new();
    private readonly ClientFactory _clientFactory;
    private readonly SubscriptionManager _subscriptionManager = new();
    private readonly BackpressureCoordinator? _backpressureCoordinator;
    private bool _disposed;

    public IBackpressureMonitor? BackpressureMonitor => _backpressureCoordinator;

    public Bus(
        ITopicManager topicManager,
        GroupCoordinator coordinator,
        IPartitioner partitionSelector,
        IRebalancer rebalanceStrategy)
    {
        _topicManager = topicManager;
        _coordinator = coordinator;
        _partitionSelector = partitionSelector;
        _clientFactory = new ClientFactory(topicManager, coordinator, partitionSelector, rebalanceStrategy);

        if (topicManager is TopicManager tm)
        {
            _backpressureCoordinator = new BackpressureCoordinator();
            tm.SetBackpressureCoordinator(_backpressureCoordinator);
            coordinator.SetBackpressureCoordinator(_backpressureCoordinator);
        }
    }

    // --- IMessageBus ---

    public async ValueTask<DeliveryResult> ProduceAsync<TMessage>(
        TopicId topic, TMessage message, string? key = null, CancellationToken ct = default)
        where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureTopicExistsAsync(topic);

        var options = _topicManager.GetTopicOptions(topic);
        var effectivePartitioner = options.Ordering == OrderingMode.PerKey && key is not null
            ? _keyHashPartitioner : _partitionSelector;
        var partition = new PartitionId(effectivePartitioner.SelectPartition(options.PartitionCount, key, []));

        return await WriteToPartitionAsync(topic, message, key, partition, ct);
    }

    public async ValueTask<DeliveryResult> ProduceAsync<TMessage>(
        TopicId topic, PartitionId partition, TMessage message, CancellationToken ct = default)
        where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureTopicExistsAsync(topic);
        return await WriteToPartitionAsync(topic, message, null, partition, ct);
    }

    public async Task SubscribeAsync<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken ct = default) where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureTopicExistsAsync(topic);
        var consumer = _clientFactory.CreateConsumer<TMessage>(topic);
        await _subscriptionManager.StartAsync(consumer, handler, autoCommit: false, ct);
    }

    public Task SubscribeAsync<TMessage>(
        TopicId topic, ConsumerGroupId groupId,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken ct = default) where TMessage : MessageBase
        => SubscribeAsync(topic, groupId, autoCommit: true, handler, ct);

    public async Task SubscribeAsync<TMessage>(
        TopicId topic, ConsumerGroupId groupId, bool autoCommit,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken ct = default) where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await EnsureTopicExistsAsync(topic);

        var partitionCount = _topicManager.GetTopicOptions(topic).PartitionCount;
        var consumer = _clientFactory.CreateGroupConsumer<TMessage>(topic, groupId);
        await _subscriptionManager.StartAsync(consumer, handler, autoCommit, ct,
            onCleanup: () =>
            {
                var (_, revoked) = _coordinator.UnregisterConsumer(groupId, consumer.ConsumerId, partitionCount);
                consumer.OnPartitionsRevoked(revoked);
            });
    }

    public IProducer<TMessage> CreateProducer<TMessage>(TopicId topic) where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureTopicExistsSync(topic);
        return _clientFactory.CreateProducer<TMessage>(topic);
    }

    public IConsumer<TMessage> CreateConsumer<TMessage>(TopicId topic, ConsumerGroupId? groupId = null)
        where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureTopicExistsSync(topic);
        return _clientFactory.CreateConsumer<TMessage>(topic, groupId);
    }

    // --- IDomainEventBus ---

    public void OnPublish<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler) where TMessage : MessageBase
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureTopicExistsSync(topic);
        _eventBus.Register(topic, handler);
    }

    public bool RemoveHandler<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler) where TMessage : MessageBase
        => _eventBus.Unregister(topic, handler);

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        await _subscriptionManager.DisposeAsync();
        _backpressureCoordinator?.Dispose();
    }

    private async ValueTask<DeliveryResult> WriteToPartitionAsync<TMessage>(
        TopicId topic, TMessage message, string? key, PartitionId partition, CancellationToken ct)
        where TMessage : MessageBase
    {
        var timestamp = DateTime.UtcNow;
        var store = _topicManager.GetPartitionStore<TMessage>(topic, partition)
            ?? throw new InvalidOperationException(
                $"Partition {partition} does not exist for topic '{topic}'.");

        var offset = await store.AppendAsync(message, key, timestamp, ct);
        await _eventBus.FireAsync<TMessage>(topic, new MessageEnvelope<TMessage>
        {
            Offset = offset,
            Partition = partition,
            Topic = topic,
            Value = message,
            Key = key,
            Timestamp = timestamp
        }, ct);

        return new DeliveryResult { Offset = offset, Partition = partition, Topic = topic, Timestamp = timestamp };
    }

    private ValueTask EnsureTopicExistsAsync(TopicId topic)
    {
        if (!_topicManager.TopicExists(topic)) _topicManager.CreateTopic(topic);
        return ValueTask.CompletedTask;
    }

    private void EnsureTopicExistsSync(TopicId topic)
    {
        if (!_topicManager.TopicExists(topic)) _topicManager.CreateTopic(topic);
    }
}
