using LiteQueue.Abstractions;

namespace LiteQueue.Models;

public sealed class ConsumeResult<TMessage> where TMessage : MessageBase
{
    public required TopicId Topic { get; init; }
    public required PartitionId Partition { get; init; }
    public required Offset Offset { get; init; }
    public required TMessage Value { get; init; }
    public string? Key { get; init; }
    public required DateTime Timestamp { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
