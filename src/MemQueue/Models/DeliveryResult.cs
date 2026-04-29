// src/MemQueue/Models/DeliveryResult.cs
namespace MemQueue.Models;

public sealed class DeliveryResult
{
    public required Offset Offset { get; init; }
    public required PartitionId Partition { get; init; }
    public required TopicId Topic { get; init; }
    public required DateTime Timestamp { get; init; }
}
