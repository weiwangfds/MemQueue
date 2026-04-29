// src/LiteQueue/Implementation/Producer.cs
using LiteQueue.Abstractions;
using LiteQueue.Models;

namespace LiteQueue.Implementation;

public sealed class Producer<TMessage> : IProducer<TMessage> where TMessage : MessageBase
{
    private readonly ITopicManager _topicManager;
    private readonly IPartitioner _partitionSelector;
    private bool _disposed;

    public TopicId Topic { get; }

    public Producer(TopicId topic, ITopicManager topicManager, IPartitioner partitionSelector)
    {
        Topic = topic;
        _topicManager = topicManager;
        _partitionSelector = partitionSelector;
    }

    public ValueTask<DeliveryResult> ProduceAsync(
        TMessage message, string? key = null, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var options = _topicManager.GetTopicOptions(Topic);
        var partition = new PartitionId(_partitionSelector.SelectPartition(options.PartitionCount, key, []));
        return ProduceToPartitionAsync(message, key, partition, ct);
    }

    public ValueTask<DeliveryResult> ProduceAsync(
        TMessage message, PartitionId partition, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return ProduceToPartitionAsync(message, null, partition, ct);
    }

    public async ValueTask<IReadOnlyList<DeliveryResult>> ProduceBatchAsync(
        IReadOnlyList<TMessage> messages, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        var results = new DeliveryResult[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            results[i] = await ProduceAsync(messages[i], ct: ct);
        return results;
    }

    public ValueTask DisposeAsync() { _disposed = true; return ValueTask.CompletedTask; }

    private async ValueTask<DeliveryResult> ProduceToPartitionAsync(
        TMessage message, string? key, PartitionId partition, CancellationToken ct)
    {
        var store = _topicManager.GetPartitionStore<TMessage>(Topic, partition)
            ?? throw new InvalidOperationException(
                $"Partition {partition} does not exist for topic '{Topic}'.");

        var timestamp = DateTime.UtcNow;
        var offset = await store.AppendAsync(message, key, timestamp, ct);

        return new DeliveryResult
        {
            Offset = offset,
            Partition = partition,
            Topic = Topic,
            Timestamp = timestamp
        };
    }
}
