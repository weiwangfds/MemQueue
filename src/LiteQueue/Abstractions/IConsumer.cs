// src/LiteQueue/Abstractions/IConsumer.cs
using LiteQueue.Models;

namespace LiteQueue.Abstractions;

public interface IConsumer<TMessage> : IAsyncDisposable where TMessage : MessageBase
{
    TopicId Topic { get; }
    ConsumerGroupId? GroupId { get; }

    ValueTask<ConsumeResult<TMessage>> ConsumeAsync(CancellationToken ct = default);
    IAsyncEnumerable<ConsumeResult<TMessage>> ConsumeAllAsync(CancellationToken ct = default);

    ValueTask CommitAsync(Offset offset, PartitionId partition, CancellationToken ct = default);
    ValueTask CommitAsync(CancellationToken ct = default);
    ValueTask SeekAsync(PartitionId partition, Offset offset, CancellationToken ct = default);
    ValueTask SeekToBeginningAsync(PartitionId partition, CancellationToken ct = default);
    ValueTask SeekToEndAsync(PartitionId partition, CancellationToken ct = default);
    long GetPosition(PartitionId partition);
}
