// src/LiteQueue/Core/TopicManager.cs  (keep filename for DI compatibility)
using System.Collections.Concurrent;
using LiteQueue.Abstractions;
using LiteQueue.Models;

namespace LiteQueue.Core;

public sealed class TopicManager : ITopicManager, IDisposable
{
    private sealed class TopicEntry(TopicOptions options)
    {
        public TopicOptions Options { get; } = options;
        public Dictionary<PartitionId, AppendOnlyLog> Partitions { get; } = new();
        private IReadOnlyList<IMessageStore>? _cachedStores;

        // P4 fix: cache the store list — created once after partitions are populated
        public IReadOnlyList<IMessageStore> GetCachedStores()
            => _cachedStores ??= Partitions.Values.Cast<IMessageStore>().ToArray();
    }

    private readonly ConcurrentDictionary<TopicId, TopicEntry> _topics = new();
    private BackpressureCoordinator? _backpressureCoordinator;
    private OrderingMode _defaultOrdering = OrderingMode.None;

    internal void SetBackpressureCoordinator(BackpressureCoordinator coordinator)
        => _backpressureCoordinator = coordinator;

    internal void SetDefaultOrdering(OrderingMode mode) => _defaultOrdering = mode;

    public void CreateTopic(TopicId topic, Action<TopicOptions>? configure = null)
    {
        var options = new TopicOptions();
        configure?.Invoke(options);
        options.Ordering ??= _defaultOrdering;

        _topics.GetOrAdd(topic, _ =>
        {
            var entry = new TopicEntry(options);
            for (var i = 0; i < options.PartitionCount; i++)
            {
                entry.Partitions[new PartitionId(i)] = new AppendOnlyLog(
                    new PartitionId(i), topic,
                    options.BufferCapacity, options.OverflowPolicy);
            }
            return entry;
        });

        if (options.Backpressure is not null && _backpressureCoordinator is not null)
            _backpressureCoordinator.SetOptions(topic, options.Backpressure);
    }

    public bool TopicExists(TopicId topic) => _topics.ContainsKey(topic);

    public TopicOptions GetTopicOptions(TopicId topic)
    {
        if (!_topics.TryGetValue(topic, out var e))
            throw new KeyNotFoundException($"Topic '{topic}' does not exist.");
        return e.Options;
    }

    public IReadOnlyCollection<TopicId> GetTopics() => _topics.Keys.ToList();

    public void DeleteTopic(TopicId topic)
    {
        if (_topics.TryRemove(topic, out var entry))
            foreach (var log in entry.Partitions.Values) log.Dispose();
    }

    public IMessageStore? GetPartitionStore(TopicId topic, PartitionId partition)
    {
        if (!_topics.TryGetValue(topic, out var e)) return null;
        return e.Partitions.TryGetValue(partition, out var log) ? log : null;
    }

    public IMessageStore<T>? GetPartitionStore<T>(TopicId topic, PartitionId partition)
        where T : MessageBase
    {
        if (!_topics.TryGetValue(topic, out var e)) return null;
        if (!e.Partitions.TryGetValue(partition, out var log)) return null;
        return new PartitionLog<T>(log, topic);
    }

    public IReadOnlyCollection<IMessageStore> GetAllPartitionStores(TopicId topic)
    {
        if (!_topics.TryGetValue(topic, out var e)) return [];
        return e.GetCachedStores();
    }

    public TopicStatistics GetTopicStatistics(TopicId topic)
    {
        if (!_topics.TryGetValue(topic, out var e))
            throw new KeyNotFoundException($"Topic '{topic}' does not exist.");

        var partitions = new Dictionary<int, PartitionStats>(e.Partitions.Count);
        long totalMessages = 0;
        double totalUtil = 0;

        foreach (var (pid, log) in e.Partitions)
        {
            var head = log.HeadOffset;
            var tail = log.TailOffset;
            var count = head.Value - tail.Value;
            totalMessages += count;
            var util = e.Options.BufferCapacity > 0 ? (double)count / e.Options.BufferCapacity : 0;
            totalUtil += util;

            partitions[pid.Value] = new PartitionStats
            {
                PartitionId = pid.Value,
                HeadOffset = head.Value,
                TailOffset = tail.Value,
                MessageCount = count,
                Utilization = util,
                ConsumerLag = 0
            };
        }

        return new TopicStatistics
        {
            Topic = topic,
            PartitionCount = e.Partitions.Count,
            TotalMessages = totalMessages,
            AverageUtilization = partitions.Count > 0 ? totalUtil / partitions.Count : 0,
            Partitions = partitions
        };
    }

    public void Dispose()
    {
        foreach (var e in _topics.Values)
            foreach (var log in e.Partitions.Values) log.Dispose();
        _topics.Clear();
    }
}
