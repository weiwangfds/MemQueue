using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using MemQueue.Abstractions;
using MemQueue.Core;
using MemQueue.Implementation;
using MemQueue.Implementation.PartitionSelectors;
using MemQueue.Implementation.RebalanceStrategies;
using MemQueue.Models;

namespace MemQueue.Benchmarks;

[MemoryDiagnoser]
[ShortRunJob]
public class ProduceConsumeBenchmarks
{
    private IMessageBus _bus = null!;
    private IProducer<TestMessage> _producer = null!;
    private List<TestMessage> _batch10 = null!;
    private List<TestMessage> _batch100 = null!;
    private List<TestMessage> _batch1000 = null!;
    private const string TopicName = "bench-pubsub";

    [Params(4, 16)]
    public int PartitionCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var topicManager = new TopicManager();
        topicManager.CreateTopic((TopicId)TopicName, opts => opts.PartitionCount = PartitionCount);

        var coordinator = new GroupCoordinator();
        var partitioner = new KeyHashPartitioner();
        var rebalancer = new RangeRebalancer();

        _bus = new Bus(topicManager, coordinator, partitioner, rebalancer);
        _producer = _bus.CreateProducer<TestMessage>((TopicId)TopicName);

        _batch10 = Enumerable.Range(0, 10).Select(i => new TestMessage($"batch-{i}")).ToList();
        _batch100 = Enumerable.Range(0, 100).Select(i => new TestMessage($"batch-{i}")).ToList();
        _batch1000 = Enumerable.Range(0, 1000).Select(i => new TestMessage($"batch-{i}")).ToList();
    }

    [Benchmark(Description = "Bus.ProduceAsync (no key)")]
    public ValueTask<DeliveryResult> Bus_Produce_NoKey()
    {
        var msg = new TestMessage($"msg-{Guid.NewGuid():N}");
        return _bus.ProduceAsync((TopicId)TopicName, msg);
    }

    [Benchmark(Description = "Bus.ProduceAsync (with key)")]
    public ValueTask<DeliveryResult> Bus_Produce_WithKey()
    {
        var msg = new TestMessage($"msg-{Guid.NewGuid():N}");
        return _bus.ProduceAsync((TopicId)TopicName, msg, $"key-{Random.Shared.Next(100)}");
    }

    [Benchmark(Description = "Producer.ProduceAsync (no key)")]
    public ValueTask<DeliveryResult> Producer_Produce_NoKey()
    {
        var msg = new TestMessage($"msg-{Guid.NewGuid():N}");
        return _producer.ProduceAsync(msg);
    }

    [Benchmark(Description = "Producer.ProduceAsync (with key)")]
    public ValueTask<DeliveryResult> Producer_Produce_WithKey()
    {
        var msg = new TestMessage($"msg-{Guid.NewGuid():N}");
        return _producer.ProduceAsync(msg, $"key-{Random.Shared.Next(100)}");
    }

    [Benchmark(Description = "Producer.ProduceBatchAsync x10")]
    public ValueTask<IReadOnlyList<DeliveryResult>> Producer_Batch_10()
    {
        return _producer.ProduceBatchAsync(_batch10);
    }

    [Benchmark(Description = "Producer.ProduceBatchAsync x100")]
    public ValueTask<IReadOnlyList<DeliveryResult>> Producer_Batch_100()
    {
        return _producer.ProduceBatchAsync(_batch100);
    }

    [Benchmark(Description = "Producer.ProduceBatchAsync x1000")]
    public ValueTask<IReadOnlyList<DeliveryResult>> Producer_Batch_1000()
    {
        return _producer.ProduceBatchAsync(_batch1000);
    }
}
