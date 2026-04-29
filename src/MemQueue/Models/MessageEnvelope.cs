using MemQueue.Abstractions;

namespace MemQueue.Models;

public sealed class MessageEnvelope<T> where T : MessageBase
{
    public required Offset Offset { get; set; }
    public required PartitionId Partition { get; set; }
    public required TopicId Topic { get; init; }
    public required T Value { get; init; }
    public string? Key { get; init; }
    public required DateTime Timestamp { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
}
