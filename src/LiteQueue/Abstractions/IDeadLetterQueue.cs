using LiteQueue.Models;

namespace LiteQueue.Abstractions;

/// <summary>
/// Dead letter queue for capturing failed message deliveries.
/// </summary>
public interface IDeadLetterQueue
{
    /// <summary>
    /// Publish a delivery error along with the failed message payload.
    /// </summary>
    void Publish(DeliveryError error, object? message);

    /// <summary>
    /// Get all delivery errors for a topic.
    /// </summary>
    IReadOnlyList<(DeliveryError Error, object? Message)> GetErrors(string topic);

    /// <summary>
    /// Clear all delivery errors for a topic.
    /// </summary>
    void Clear(string topic);
}
