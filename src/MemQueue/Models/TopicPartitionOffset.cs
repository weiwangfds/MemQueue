namespace MemQueue.Models;

/// <summary>
/// Represents a (topic, partition, offset) triple. Mirrors Kafka's TopicPartitionOffset.
/// Used for seek, commit, and offset tracking operations.
/// </summary>
public readonly record struct TopicPartitionOffset(string Topic, int Partition, long Offset)
{
    public TopicPartition TopicPartition => new(Topic, Partition);
}
