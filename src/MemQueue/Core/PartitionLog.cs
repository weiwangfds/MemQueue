// src/MemQueue/Core/PartitionLog.cs
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Core;

/// <summary>
/// Typed facade over AppendOnlyLog. Created on demand by TopicRegistry.GetPartitionStore&lt;T&gt;.
/// B3 fix: WaitForMessageAsync returns only the exact requested offset.
/// </summary>
public sealed class PartitionLog<T> : IMessageStore<T>, IDisposable where T : MessageBase
{
    private readonly AppendOnlyLog _log;
    private readonly TopicId _topic;

    internal PartitionLog(AppendOnlyLog log, TopicId topic)
    {
        _log = log;
        _topic = topic;
    }

    public PartitionId PartitionId => _log.PartitionId;
    public Offset HeadOffset => _log.HeadOffset;
    public Offset TailOffset => _log.TailOffset;

    public ValueTask<Offset> AppendAsync(T message, string? key, DateTime timestamp, CancellationToken ct)
        => _log.AppendAsync(message, key, timestamp, null, ct);

    public MessageEnvelope<T>? Read(Offset offset)
    {
        var raw = _log.ReadRaw(offset);
        if (raw is null) return null;
        return Map(raw.Value, offset);
    }

    public IReadOnlyList<MessageEnvelope<T>> ReadRange(Offset from, Offset to)
    {
        var raws = _log.ReadRangeRaw(from, to);
        if (raws.Count == 0) return [];

        var result = new List<MessageEnvelope<T>>(raws.Count);
        var current = from;
        foreach (var raw in raws)
        {
            result.Add(Map(raw, current));
            current = current.Next();
        }
        return result;
    }

    // B3 fix: spin on exact offset — never return envelope with wrong offset
    public async ValueTask<MessageEnvelope<T>> WaitForMessageAsync(Offset offset, CancellationToken ct)
    {
        while (true)
        {
            var msg = Read(offset);
            if (msg is not null) return msg;
            await _log.Notifier.WaitAsync(ct);
        }
    }

    public int ApplyRetention(RetentionPolicy policy) => _log.ApplyRetention(policy);

    public void Dispose() { /* AppendOnlyLog lifetime owned by TopicRegistry */ }

    private MessageEnvelope<T> Map(StoredEntry raw, Offset offset) => new()
    {
        Offset = offset,
        Partition = _log.PartitionId,
        Topic = _topic,
        Value = (T)raw.Payload,
        Key = raw.Key,
        Timestamp = raw.Timestamp,
        Headers = raw.Headers
    };
}
