namespace MemQueue.Abstractions;

/// <summary>
/// Abstract record base for all messages. Inherit from this to define message types.
/// Enforces record semantics (immutability, value equality) for zero-copy data transfer.
/// </summary>
/// <example>
/// <code>
/// public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;
/// </code>
/// </example>
public abstract record MessageBase;
