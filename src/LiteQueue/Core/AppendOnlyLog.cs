using LiteQueue.Abstractions;
using LiteQueue.Exceptions;
using LiteQueue.Models;
using Nito.AsyncEx;

namespace LiteQueue.Core;

internal readonly record struct StoredEntry(
    object Payload,
    string? Key,
    DateTime Timestamp,
    IReadOnlyDictionary<string, string>? Headers);

/// <summary>
/// Non-generic ring buffer. Owns physical storage, notification signal, and retention logic.
/// PartitionLog&lt;T&gt; wraps this and projects to typed MessageEnvelope&lt;T&gt;.
///
/// Bug fixes implemented here:
/// B2: DropNewest validates capacity BEFORE claiming an offset slot
/// B5: Read/ReadRange hold retention read lock to prevent null-slot TOCTOU
/// B6: AdvanceTail releases _writeGate for Block mode
/// P2: Single write lock covers both ByCount and ByAge in ByCountOrAge
/// </summary>
internal sealed class AppendOnlyLog : IMessageStore, IDisposable
{
    private readonly StoredEntry?[] _entries;
    private readonly int _capacity;
    private readonly OverflowPolicy _overflowPolicy;
    private readonly SemaphoreSlim? _writeGate;
    private readonly AsyncReaderWriterLock _retentionLock = new();
    private readonly MessageNotifier _notifier = new();
    private readonly TopicId _topic;
    private long _headOffset;
    private long _tailOffset;
    private bool _disposed;

    public PartitionId PartitionId { get; }
    public Offset HeadOffset => new(Volatile.Read(ref _headOffset));
    public Offset TailOffset => new(Volatile.Read(ref _tailOffset));
    internal MessageNotifier Notifier => _notifier;

    public AppendOnlyLog(
        PartitionId partitionId,
        TopicId topic,
        int capacity = 1024,
        OverflowPolicy overflowPolicy = OverflowPolicy.OverwriteOldest)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        PartitionId = partitionId;
        _topic = topic;
        _capacity = capacity;
        _overflowPolicy = overflowPolicy;
        _entries = new StoredEntry?[capacity];
        _writeGate = overflowPolicy == OverflowPolicy.Block
            ? new SemaphoreSlim(capacity, capacity)
            : null;
    }

    // B2 fix: check capacity BEFORE incrementing _headOffset for DropNewest
    public async ValueTask<Offset> AppendAsync(
        object payload,
        string? key,
        DateTime timestamp,
        IReadOnlyDictionary<string, string>? headers,
        CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_writeGate is not null)
            await _writeGate.WaitAsync(ct);

        // B2: validate before claiming the offset slot
        if (_overflowPolicy == OverflowPolicy.DropNewest)
        {
            var tail = Volatile.Read(ref _tailOffset);
            var head = Volatile.Read(ref _headOffset);
            if (head - tail >= _capacity)
            {
                _writeGate?.Release(1);
                throw new BufferOverflowException(
                    $"Partition {PartitionId} is full (capacity: {_capacity}). Message rejected.");
            }
        }

        var rawOffset = Interlocked.Increment(ref _headOffset) - 1;
        _entries[rawOffset % _capacity] = new StoredEntry(payload, key, timestamp, headers);

        if (_overflowPolicy == OverflowPolicy.OverwriteOldest)
            AdvanceTail(rawOffset + 1 - _capacity);

        _notifier.Signal();

        return new Offset(rawOffset);
    }

    public StoredEntry? ReadRaw(Offset offset)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = _retentionLock.ReaderLock();
        var head = Volatile.Read(ref _headOffset);
        var tail = Volatile.Read(ref _tailOffset);
        if (offset.Value < tail || offset.Value >= head) return null;
        return _entries[offset.Value % _capacity];
    }

    public IReadOnlyList<StoredEntry> ReadRangeRaw(Offset from, Offset to)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = _retentionLock.ReaderLock();
        var tail = Volatile.Read(ref _tailOffset);
        var head = Volatile.Read(ref _headOffset);
        var start = Math.Max(from.Value, tail);
        var end = Math.Min(to.Value, head);
        if (start >= end) return [];

        var result = new List<StoredEntry>((int)(end - start));
        for (var i = start; i < end; i++)
        {
            var e = _entries[i % _capacity];
            if (e.HasValue) result.Add(e.Value);
        }
        return result;
    }

    // B5 fix: hold read lock so retention cannot null out a slot mid-read
    public async ValueTask<StoredEntry?> ReadRawAsync(Offset offset, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = await _retentionLock.ReaderLockAsync(ct);
        var head = Volatile.Read(ref _headOffset);
        var tail = Volatile.Read(ref _tailOffset);
        if (offset.Value < tail || offset.Value >= head) return null;
        return _entries[offset.Value % _capacity];
    }

    public async ValueTask<IReadOnlyList<StoredEntry>> ReadRangeRawAsync(
        Offset from, Offset to, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = await _retentionLock.ReaderLockAsync(ct);
        var tail = Volatile.Read(ref _tailOffset);
        var head = Volatile.Read(ref _headOffset);
        var start = Math.Max(from.Value, tail);
        var end = Math.Min(to.Value, head);
        if (start >= end) return [];

        var result = new List<StoredEntry>((int)(end - start));
        for (var i = start; i < end; i++)
        {
            var e = _entries[i % _capacity];
            if (e.HasValue) result.Add(e.Value);
        }
        return result;
    }

    // P2 fix: single write lock covers both ByCount and ByAge when ByCountOrAge
    public int ApplyRetention(RetentionPolicy policy)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = _retentionLock.WriterLock();
        return policy switch
        {
            RetentionPolicy.ByCount(var max)               => TrimByCount(max),
            RetentionPolicy.ByAge(var age)                 => TrimByAge(age),
            RetentionPolicy.ByCountOrAge(var max, var age) => TrimByCount(max) + TrimByAge(age),
            _                                              => 0
        };
    }

    public async ValueTask<int> ApplyRetentionAsync(RetentionPolicy policy, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        using var _ = await _retentionLock.WriterLockAsync(ct);
        return policy switch
        {
            RetentionPolicy.ByCount(var max)               => TrimByCount(max),
            RetentionPolicy.ByAge(var age)                 => TrimByAge(age),
            RetentionPolicy.ByCountOrAge(var max, var age) => TrimByCount(max) + TrimByAge(age),
            _                                              => 0
        };
    }

    private int TrimByCount(int maxKeep)
    {
        var head = Volatile.Read(ref _headOffset);
        var tail = Volatile.Read(ref _tailOffset);
        var current = head - tail;
        if (current <= maxKeep) return 0;

        var newTail = head - maxKeep;
        var removed = 0;
        for (var i = tail; i < newTail; i++)
        {
            if (_entries[i % _capacity].HasValue)
            {
                _entries[i % _capacity] = null;
                removed++;
            }
        }
        AdvanceTail(newTail);
        return removed;
    }

    private int TrimByAge(TimeSpan maxAge)
    {
        var cutoff = DateTime.UtcNow - maxAge;
        var tail = Volatile.Read(ref _tailOffset);
        var head = Volatile.Read(ref _headOffset);
        var newTail = tail;
        var removed = 0;

        for (var i = tail; i < head; i++)
        {
            var e = _entries[i % _capacity];
            if (!e.HasValue) continue;
            if (e.Value.Timestamp < cutoff)
            {
                _entries[i % _capacity] = null;
                removed++;
                newTail = i + 1;
            }
            else break;
        }
        AdvanceTail(newTail);
        return removed;
    }

    // B6 fix: AdvanceTail releases _writeGate for Block mode
    private void AdvanceTail(long minTail)
    {
        if (minTail <= 0) return;
        var spin = new SpinWait();
        while (true)
        {
            var current = Volatile.Read(ref _tailOffset);
            if (minTail <= current) break;
            if (Interlocked.CompareExchange(ref _tailOffset, minTail, current) == current)
            {
                var freed = (int)(minTail - current);
                if (freed > 0 && _writeGate is not null)
                {
                    try { _writeGate.Release(freed); }
                    catch (SemaphoreFullException) { /* already at max */ }
                }
                break;
            }
            spin.SpinOnce();
        }
    }

    internal void AdvanceTailTo(Offset newTail)
    {
        var current = Volatile.Read(ref _tailOffset);
        if (newTail.Value <= current) return;

        using var _ = _retentionLock.WriterLock();
        var head = Volatile.Read(ref _headOffset);
        current = Volatile.Read(ref _tailOffset);
        for (var i = current; i < newTail.Value && i < head; i++)
            _entries[i % _capacity] = null;
        AdvanceTail(newTail.Value);
    }

    internal async ValueTask AdvanceTailToAsync(Offset newTail, CancellationToken ct)
    {
        var current = Volatile.Read(ref _tailOffset);
        if (newTail.Value <= current) return;

        using var _ = await _retentionLock.WriterLockAsync(ct);
        var head = Volatile.Read(ref _headOffset);
        current = Volatile.Read(ref _tailOffset);
        for (var i = current; i < newTail.Value && i < head; i++)
            _entries[i % _capacity] = null;
        AdvanceTail(newTail.Value);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _notifier.Dispose();
        using (_retentionLock.WriterLock())
        {
            Array.Clear(_entries);
        }
        _writeGate?.Dispose();
    }
}
