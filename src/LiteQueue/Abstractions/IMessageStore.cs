// src/LiteQueue/Abstractions/IMessageStore.cs
using LiteQueue.Models;

namespace LiteQueue.Abstractions;

/// <summary>Non-generic base. Used by RetentionManager and statistics.</summary>
public interface IMessageStore
{
    PartitionId PartitionId { get; }
    Offset HeadOffset { get; }
    Offset TailOffset { get; }
    int ApplyRetention(RetentionPolicy policy);
}

/// <summary>Typed store. Used by Consumer&lt;T&gt; and Producer&lt;T&gt;.</summary>
public interface IMessageStore<T> : IMessageStore where T : MessageBase
{
    ValueTask<Offset> AppendAsync(T message, string? key, DateTime timestamp, CancellationToken ct);
    MessageEnvelope<T>? Read(Offset offset);
    IReadOnlyList<MessageEnvelope<T>> ReadRange(Offset from, Offset to);
    ValueTask<MessageEnvelope<T>> WaitForMessageAsync(Offset offset, CancellationToken ct);
}
