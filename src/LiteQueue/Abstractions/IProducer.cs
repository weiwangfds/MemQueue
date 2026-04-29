// src/LiteQueue/Abstractions/IProducer.cs
using LiteQueue.Models;

namespace LiteQueue.Abstractions;

public interface IProducer<TMessage> : IAsyncDisposable where TMessage : MessageBase
{
    TopicId Topic { get; }

    ValueTask<DeliveryResult> ProduceAsync(
        TMessage message,
        string? key = null,
        CancellationToken cancellationToken = default);

    ValueTask<DeliveryResult> ProduceAsync(
        TMessage message,
        PartitionId partition,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<DeliveryResult>> ProduceBatchAsync(
        IReadOnlyList<TMessage> messages,
        CancellationToken cancellationToken = default);
}
