namespace MemQueue.Models;

/// <summary>
/// Represents a (topic, partition) pair. Mirrors Kafka's TopicPartition.
/// </summary>
public readonly record struct TopicPartition(string Topic, int Partition);
