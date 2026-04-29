// src/MemQueue/Abstractions/ITopicManager.cs
using MemQueue.Models;

namespace MemQueue.Abstractions;

public interface ITopicManager
{
    void CreateTopic(TopicId topic, Action<TopicOptions>? configure = null);
    bool TopicExists(TopicId topic);
    TopicOptions GetTopicOptions(TopicId topic);
    IReadOnlyCollection<TopicId> GetTopics();
    void DeleteTopic(TopicId topic);

    /// <summary>Returns non-generic store — for RetentionManager and statistics.</summary>
    IMessageStore? GetPartitionStore(TopicId topic, PartitionId partition);

    /// <summary>Returns typed store — for Consumer&lt;T&gt; and Producer&lt;T&gt;.</summary>
    IMessageStore<T>? GetPartitionStore<T>(TopicId topic, PartitionId partition) where T : MessageBase;

    IReadOnlyCollection<IMessageStore> GetAllPartitionStores(TopicId topic);
    TopicStatistics GetTopicStatistics(TopicId topic);
}
