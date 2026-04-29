using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using LiteQueue.Abstractions;
using LiteQueue.Core;
using LiteQueue.Models;

namespace LiteQueue.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class PartitionLogBenchmarks
{
    private AppendOnlyLog _log = null!;
    private PartitionLog<TestMessage> _pl = null!;
    private TestMessage _message = null!;
    private long _maxOffset;
    private const int Capacity = 100_000;

    [Params(1_000, 10_000)]
    public int PrePopulateCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _log = new AppendOnlyLog(new PartitionId(0), (TopicId)"bench-topic", Capacity);
        _pl = new PartitionLog<TestMessage>(_log, (TopicId)"bench-topic");
        _message = new TestMessage("benchmark-payload");

        for (var i = 0; i < PrePopulateCount; i++)
        {
            _pl.AppendAsync(new TestMessage($"msg-{i}"), null, DateTime.UtcNow, default).AsTask().GetAwaiter().GetResult();
        }

        _maxOffset = _log.HeadOffset.Value;
    }

    [Benchmark(Description = "AppendAsync single message")]
    public long AppendAsync()
    {
        return _pl.AppendAsync(_message, null, DateTime.UtcNow, default).AsTask().GetAwaiter().GetResult();
    }

    [Benchmark(Description = "Read single message")]
    public MessageEnvelope<TestMessage>? Read()
    {
        var offset = Random.Shared.Next((int)_maxOffset);
        return _pl.Read(new Offset(offset));
    }

    [Benchmark(Description = "ReadRange 10 messages")]
    public IReadOnlyList<MessageEnvelope<TestMessage>> ReadRange_10()
    {
        var from = Random.Shared.Next((int)Math.Max(0, _maxOffset - 10));
        return _pl.ReadRange(new Offset(from), new Offset(from + 10));
    }

    [Benchmark(Description = "ReadRange 100 messages")]
    public IReadOnlyList<MessageEnvelope<TestMessage>> ReadRange_100()
    {
        var from = Random.Shared.Next((int)Math.Max(0, _maxOffset - 100));
        return _pl.ReadRange(new Offset(from), new Offset(from + 100));
    }

    [Benchmark(Description = "ReadRange 1000 messages")]
    public IReadOnlyList<MessageEnvelope<TestMessage>> ReadRange_1000()
    {
        var from = Random.Shared.Next((int)Math.Max(0, _maxOffset - 1000));
        return _pl.ReadRange(new Offset(from), new Offset(from + 1000));
    }

    [Benchmark(Description = "ApplyRetention by count")]
    public int ApplyRetention_ByCount()
    {
        var policy = RetentionPolicy.ByCount.Of(PrePopulateCount / 2);
        return _pl.ApplyRetention(policy);
    }
}

public record TestMessage(string Value) : MessageBase;
