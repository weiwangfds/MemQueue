using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using MemQueue.Abstractions;
using MemQueue.Implementation.RebalanceStrategies;

namespace MemQueue.Benchmarks;

/// <summary>
/// 再平衡策略基准测试，对比 Range 和 RoundRobin 两种分区分配算法。
/// Range: 将分区按连续区间分配给排序后的消费者，时间复杂度 O(P + C)。
/// RoundRobin: 逐个分区轮询分配给消费者，时间复杂度 O(P + C·logC)（含排序）。
/// 两个参数维度: 分区数 (4–256) × 消费者数 (1–50)，覆盖从单消费者到大规模消费者组。
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
public class RebalanceBenchmarks
{
    private RangeRebalancer _range = null!;
    private RoundRobinRebalancer _roundRobin = null!;
    private IReadOnlyList<string> _consumerIds = null!;

    [Params(4, 16, 64, 256)]
    public int PartitionCount { get; set; }

    [Params(1, 5, 10, 50)]
    public int ConsumerCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _range = new RangeRebalancer();
        _roundRobin = new RoundRobinRebalancer();
        _consumerIds = Enumerable.Range(0, ConsumerCount)
            .Select(i => $"consumer-{i}")
            .ToList();
    }

    /// <summary>
    /// Range 策略分区分配。将 partitionCount 个分区按连续区间均匀分给 consumerCount 个消费者。
    /// 无法整除时，前 (partitionCount % consumerCount) 个消费者多分配 1 个分区。
    /// </summary>
    [Benchmark(Description = "RangeRebalancer.Assign")]
    public IReadOnlyDictionary<string, IReadOnlyList<int>> Range_Assign()
    {
        return _range.Assign(PartitionCount, _consumerIds);
    }

    /// <summary>
    /// RoundRobin 策略分区分配。按轮询方式将每个分区依次分配给下一个消费者。
    /// 内部先排序消费者 ID，再逐个分配，比 Range 策略多一次 List.Add 调用/分区。
    /// </summary>
    [Benchmark(Description = "RoundRobinRebalancer.Assign")]
    public IReadOnlyDictionary<string, IReadOnlyList<int>> RoundRobin_Assign()
    {
        return _roundRobin.Assign(PartitionCount, _consumerIds);
    }
}
