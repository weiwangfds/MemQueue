using LiteQueue.Models;

namespace LiteQueue.Abstractions;

/// <summary>
/// Handler interface for messages.
/// </summary>
public interface IMessageHandler<TMessage> where TMessage : MessageBase
{
    /// <summary>
    /// Handle a message.
    /// </summary>
    ValueTask HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken = default);
}
