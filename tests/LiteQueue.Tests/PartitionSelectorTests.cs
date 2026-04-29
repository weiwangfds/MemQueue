using System.Text;
using LiteQueue.Abstractions;
using LiteQueue.Implementation.PartitionSelectors;
using Xunit;

namespace LiteQueue.Tests;

public class PartitionSelectorTests
{
    [Fact]
    public void RoundRobin_CyclesPartitions()
    {
        var selector = new RoundRobinPartitioner();
        var results = new int[6];
        for (int i = 0; i < 6; i++)
            results[i] = selector.SelectPartition(3, null, ReadOnlySpan<byte>.Empty);

        Assert.Equal(new[] { 1, 2, 0, 1, 2, 0 }, results);
    }

    [Fact]
    public void KeyHash_SameKeySamePartition()
    {
        var selector = new KeyHashPartitioner();
        var key = "my-routing-key";
        var keyBytes = Encoding.UTF8.GetBytes(key);

        var p1 = selector.SelectPartition(10, key, keyBytes);
        var p2 = selector.SelectPartition(10, key, keyBytes);
        var p3 = selector.SelectPartition(10, key, keyBytes);

        Assert.Equal(p1, p2);
        Assert.Equal(p2, p3);
        Assert.InRange(p1, 0, 9);
    }

    [Fact]
    public void KeyHash_DifferentKeys_MayDiffer()
    {
        var selector = new KeyHashPartitioner();
        var partitions = new HashSet<int>();

        for (int i = 0; i < 20; i++)
        {
            var key = $"key-{i}";
            var p = selector.SelectPartition(10, key, Encoding.UTF8.GetBytes(key));
            partitions.Add(p);
        }

        Assert.True(partitions.Count > 1, "Different keys should map to different partitions at least sometimes");
    }

    [Fact]
    public void KeyHash_NullKey_FallsBack()
    {
        var selector = new KeyHashPartitioner();
        var p1 = selector.SelectPartition(4, null, ReadOnlySpan<byte>.Empty);
        var p2 = selector.SelectPartition(4, null, ReadOnlySpan<byte>.Empty);

        Assert.InRange(p1, 0, 3);
        Assert.InRange(p2, 0, 3);
        Assert.Equal(p1 + 1 == 4 ? 0 : p1 + 1, p2);
    }

    [Fact]
    public async Task RoundRobin_ThreadSafety()
    {
        var selector = new RoundRobinPartitioner();
        var results = new int[100];
        var tasks = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() => results[i] = selector.SelectPartition(4, null, ReadOnlySpan<byte>.Empty)));

        await Task.WhenAll(tasks);

        Assert.All(results, r => Assert.InRange(r, 0, 3));
    }
}
