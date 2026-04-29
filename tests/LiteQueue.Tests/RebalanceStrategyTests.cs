using LiteQueue.Abstractions;
using LiteQueue.Implementation.RebalanceStrategies;
using Xunit;

namespace LiteQueue.Tests;

public class RebalanceStrategyTests
{
    [Fact]
    public void RangeStrategy_EvenDistribution()
    {
        var strategy = new RangeRebalancer();
        var consumers = new[] { "c-a", "c-b", "c-c" };

        var result = strategy.Assign(6, consumers);

        Assert.Equal(3, result.Count);
        Assert.All(result.Values, v => Assert.Equal(2, v.Count));

        var all = result.Values.SelectMany(v => v).OrderBy(p => p).ToList();
        Assert.Equal(new[] { 0, 1, 2, 3, 4, 5 }, all);
    }

    [Fact]
    public void RangeStrategy_UnevenDistribution()
    {
        var strategy = new RangeRebalancer();
        var consumers = new[] { "c-a", "c-b", "c-c" };

        var result = strategy.Assign(7, consumers);

        Assert.Equal(3, result.Count);
        var sortedConsumer = result.Keys.OrderBy(k => k).First();
        Assert.Equal(3, result[sortedConsumer].Count);

        var all = result.Values.SelectMany(v => v).OrderBy(p => p).ToList();
        Assert.Equal(Enumerable.Range(0, 7), all);
    }

    [Fact]
    public void RoundRobinStrategy_DistributesCorrectly()
    {
        var strategy = new RoundRobinRebalancer();
        var consumers = new[] { "c-a", "c-b", "c-c" };

        var result = strategy.Assign(4, consumers);

        Assert.Equal(3, result.Count);

        var all = result.Values.SelectMany(v => v).OrderBy(p => p).ToList();
        Assert.Equal(new[] { 0, 1, 2, 3 }, all);

        var counts = result.Values.Select(v => v.Count).OrderBy(c => c).ToList();
        Assert.Equal(new[] { 1, 1, 2 }, counts);
    }

    [Fact]
    public void RangeStrategy_EmptyConsumers_ReturnsEmpty()
    {
        var strategy = new RangeRebalancer();
        var result = strategy.Assign(4, Array.Empty<string>());
        Assert.Empty(result);
    }

    [Fact]
    public void RoundRobinStrategy_EmptyConsumers_ReturnsEmpty()
    {
        var strategy = new RoundRobinRebalancer();
        var result = strategy.Assign(4, Array.Empty<string>());
        Assert.Empty(result);
    }

    [Fact]
    public void RangeStrategy_SingleConsumer_GetsAll()
    {
        var strategy = new RangeRebalancer();
        var result = strategy.Assign(5, new[] { "only-one" });

        Assert.Single(result);
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, result["only-one"]);
    }
}
