namespace LiteQueue.Models;

/// <summary>
/// Represents a message delivery error recorded in the dead letter queue.
/// </summary>
public sealed class DeliveryError
{
    public required string Topic { get; init; }
    public required int Partition { get; init; }
    public required long Offset { get; init; }
    public required string ErrorType { get; init; }
    public required string ErrorMessage { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}
