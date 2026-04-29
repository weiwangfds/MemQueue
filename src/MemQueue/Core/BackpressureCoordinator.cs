// src/MemQueue/Core/BackpressureCoordinator.cs
using System.Collections.Concurrent;
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Core;

public sealed class BackpressureCoordinator : IBackpressureMonitor, IDisposable
{
    private readonly ConcurrentDictionary<(TopicId, PartitionId), PartitionWatermark> _watermarks = new();
    private readonly ConcurrentDictionary<(TopicId, PartitionId), AppendOnlyLog> _logs = new();
    private readonly ConcurrentDictionary<TopicId, BackpressureOptions> _options = new();
    private readonly ConcurrentDictionary<(TopicId, PartitionId), bool> _wasAboveHigh = new();

    public event Action<string, int, double>? HighWatermarkReached;
    public event Action<string, int, double>? LowWatermarkRecovered;
    public event Action<string, int, string, long>? SlowConsumerDetected;

    internal void RegisterPartition(TopicId topic, PartitionId partition, AppendOnlyLog log)
    {
        _logs[(topic, partition)] = log;
        _watermarks.TryAdd((topic, partition), new PartitionWatermark());
    }

    internal void SetOptions(TopicId topic, BackpressureOptions? options)
    {
        if (options is not null) _options[topic] = options;
    }

    internal void OnOffsetCommitted(TopicId topic, ConsumerGroupId groupId, PartitionId partition, Offset offset)
    {
        var key = (topic, partition);
        if (!_watermarks.TryGetValue(key, out var wm)) return;
        wm.Update(groupId, offset);
        var min = wm.GetMin();
        if (!min.IsValid) return;
        var newTail = new Offset(min.Value + 1);
        if (_logs.TryGetValue(key, out var log))
        {
            log.AdvanceTailTo(newTail);
            var opts = _options.TryGetValue(topic, out var o) ? o : new BackpressureOptions();
            if (opts.SlowConsumerStrategy != SlowConsumerStrategy.None)
            {
                var lag = log.HeadOffset.Value - offset.Value;
                if (opts.SlowConsumerLagThreshold > 0 && lag > opts.SlowConsumerLagThreshold)
                    SafeInvoke(() => SlowConsumerDetected?.Invoke(topic, partition, groupId, lag));
            }
        }
    }

    internal void OnGroupRemoved(TopicId topic, ConsumerGroupId groupId, int partitionCount)
    {
        for (var i = 0; i < partitionCount; i++)
        {
            var key = (topic, new PartitionId(i));
            if (!_watermarks.TryGetValue(key, out var wm)) continue;
            wm.Remove(groupId);
            var min = wm.GetMin();
            if (!min.IsValid) continue;
            if (_logs.TryGetValue(key, out var log))
                log.AdvanceTailTo(new Offset(min.Value + 1));
        }
    }

    internal void CheckWatermark(TopicId topic, PartitionId partition, double utilization)
    {
        var key = (topic, partition);
        var wasAbove = _wasAboveHigh.GetValueOrDefault(key, false);
        var opts = _options.TryGetValue(topic, out var o) ? o : new BackpressureOptions();
        if (!wasAbove && utilization >= opts.HighWatermarkPercent)
        {
            _wasAboveHigh[key] = true;
            SafeInvoke(() => HighWatermarkReached?.Invoke(topic, partition, utilization));
        }
        else if (wasAbove && utilization <= opts.LowWatermarkPercent)
        {
            _wasAboveHigh[key] = false;
            SafeInvoke(() => LowWatermarkRecovered?.Invoke(topic, partition, utilization));
        }
    }

    internal void UpdateStats(TopicId topic, PartitionId partition, double utilization, Offset headOffset)
    {
        if (_watermarks.TryGetValue((topic, partition), out var wm))
        {
            wm.LastUtilization = utilization;
            wm.HeadOffset = headOffset;
        }
    }

    public double GetUtilization(string topic, int partition)
        => _watermarks.TryGetValue(((TopicId)topic, new PartitionId(partition)), out var wm)
            ? wm.LastUtilization : 0;

    public long GetConsumerLag(string topic, int partition)
        => _watermarks.TryGetValue(((TopicId)topic, new PartitionId(partition)), out var wm)
            ? wm.GetLag() : -1;

    public int GetBlockedProducerCount(string topic, int partition)
        => _watermarks.TryGetValue(((TopicId)topic, new PartitionId(partition)), out var wm)
            ? wm.BlockedCount : 0;

    public void Dispose()
    {
        _watermarks.Clear();
        _logs.Clear();
        _options.Clear();
    }

    private static void SafeInvoke(Action action)
    {
        try { action(); }
        catch { /* isolate user event handler failures from library internals */ }
    }
}

internal sealed class PartitionWatermark
{
    private readonly ConcurrentDictionary<ConsumerGroupId, Offset> _groupOffsets = new();
    public double LastUtilization { get; set; }
    public Offset HeadOffset { get; set; }
    public int BlockedCount { get; set; }

    public void Update(ConsumerGroupId groupId, Offset offset)
        => _groupOffsets.AddOrUpdate(groupId, offset, (_, ex) => ex.Value > offset.Value ? ex : offset);

    public void Remove(ConsumerGroupId groupId) => _groupOffsets.TryRemove(groupId, out _);

    public Offset GetMin()
    {
        if (_groupOffsets.IsEmpty) return Offset.Unset;
        var min = long.MaxValue;
        foreach (var v in _groupOffsets.Values) if (v.Value < min) min = v.Value;
        return min == long.MaxValue ? Offset.Unset : new Offset(min);
    }

    public long GetLag()
    {
        var min = GetMin();
        if (!min.IsValid) return -1;
        return Math.Max(0, HeadOffset.Value - min.Value);
    }
}
