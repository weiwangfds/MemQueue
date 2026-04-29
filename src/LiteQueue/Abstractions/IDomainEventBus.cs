// src/LiteQueue/Abstractions/IDomainEventBus.cs
using LiteQueue.Models;

namespace LiteQueue.Abstractions;

/// <summary>
/// In-process fire-and-forget event bus. At-most-once. No partitioning.
/// </summary>
public interface IDomainEventBus
{
    void OnPublish<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler)
        where TMessage : MessageBase;

    bool RemoveHandler<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler)
        where TMessage : MessageBase;
}
