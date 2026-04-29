// src/MemQueue/Abstractions/IMessageBus.cs
using MemQueue.Models;

namespace MemQueue.Abstractions;

/// <summary>
/// Message bus with partition/offset/consumer-group semantics. At-least-once delivery.
/// </summary>
public interface IMessageBus : IAsyncDisposable
{
    IProducer<TMessage> CreateProducer<TMessage>(TopicId topic)
        where TMessage : MessageBase;

    IConsumer<TMessage> CreateConsumer<TMessage>(
        TopicId topic,
        ConsumerGroupId? groupId = null)
        where TMessage : MessageBase;

    Task SubscribeAsync<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
        where TMessage : MessageBase;

    Task SubscribeAsync<TMessage>(
        TopicId topic,
        ConsumerGroupId groupId,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
        where TMessage : MessageBase;

    Task SubscribeAsync<TMessage>(
        TopicId topic,
        ConsumerGroupId groupId,
        bool autoCommit,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler,
        CancellationToken cancellationToken = default)
        where TMessage : MessageBase;

    ValueTask<DeliveryResult> ProduceAsync<TMessage>(
        TopicId topic,
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default)
        where TMessage : MessageBase;

    ValueTask<DeliveryResult> ProduceAsync<TMessage>(
        TopicId topic,
        PartitionId partition,
        TMessage message,
        CancellationToken cancellationToken = default)
        where TMessage : MessageBase;

    IBackpressureMonitor? BackpressureMonitor { get; }
}
