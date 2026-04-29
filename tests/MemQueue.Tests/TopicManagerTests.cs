using MemQueue.Core;
using MemQueue.Models;
using Xunit;

namespace MemQueue.Tests;

public class TopicManagerTests
{
    private static TopicManager CreateTopicManager() => new();

    [Fact]
    public void CreateTopic_CreatesPartitions()
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"orders", o => o.PartitionCount = 4);

        var options = tm.GetTopicOptions((TopicId)"orders");
        Assert.Equal(4, options.PartitionCount);
    }

    [Fact]
    public void CreateTopic_Idempotent()
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"orders", o => o.PartitionCount = 4);
        tm.CreateTopic((TopicId)"orders", o => o.PartitionCount = 2);

        var options = tm.GetTopicOptions((TopicId)"orders");
        Assert.Equal(4, options.PartitionCount);
    }

    [Theory]
    [InlineData("existing-topic", true)]
    [InlineData("non-existing-topic", false)]
    public void TopicExists_ReturnsCorrectly(string topic, bool expected)
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"existing-topic");

        Assert.Equal(expected, tm.TopicExists((TopicId)topic));
    }

    [Fact]
    public void GetTopicOptions_ThrowsForMissing()
    {
        using var tm = CreateTopicManager();
        Assert.Throws<KeyNotFoundException>(() => tm.GetTopicOptions((TopicId)"nope"));
    }

    [Fact]
    public void DeleteTopic_RemovesTopic()
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"orders");
        Assert.True(tm.TopicExists((TopicId)"orders"));

        tm.DeleteTopic((TopicId)"orders");
        Assert.False(tm.TopicExists((TopicId)"orders"));
    }

    [Fact]
    public void GetTopics_ReturnsAllTopicNames()
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"a");
        tm.CreateTopic((TopicId)"b");
        tm.CreateTopic((TopicId)"c");

        var topics = tm.GetTopics();
        Assert.Equal(3, topics.Count);
        Assert.Contains((TopicId)"a", topics);
        Assert.Contains((TopicId)"b", topics);
        Assert.Contains((TopicId)"c", topics);
    }

    [Fact]
    public void DeleteTopic_ThenRecreate_Works()
    {
        using var tm = CreateTopicManager();
        tm.CreateTopic((TopicId)"orders", o => o.PartitionCount = 2);
        tm.DeleteTopic((TopicId)"orders");

        tm.CreateTopic((TopicId)"orders", o => o.PartitionCount = 8);
        var options = tm.GetTopicOptions((TopicId)"orders");
        Assert.Equal(8, options.PartitionCount);
    }
}
