// src/MemQueue/Models/MessageContext.cs
namespace MemQueue.Models;

public sealed class MessageContext
{
    public required TopicId Topic { get; init; }
    public required PartitionId Partition { get; init; }
    public required Offset Offset { get; init; }
    public string? Key { get; init; }
    public required DateTime Timestamp { get; init; }
    public Func<Offset, PartitionId, CancellationToken, ValueTask>? CommitFunc { get; init; }

    public ValueTask CommitAsync(CancellationToken ct = default)
        => CommitFunc is not null ? CommitFunc(Offset, Partition, ct) : ValueTask.CompletedTask;
}
