using System.Collections.Concurrent;
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Internal;

internal sealed class InMemoryDeadLetterQueue : IDeadLetterQueue
{
    private readonly ConcurrentDictionary<string, List<(DeliveryError, object?)>> _errors = new();
    private readonly int _maxErrorsPerTopic;

    public InMemoryDeadLetterQueue(int maxErrorsPerTopic = 100)
    {
        _maxErrorsPerTopic = maxErrorsPerTopic;
    }

    public void Publish(DeliveryError error, object? message)
    {
        var list = _errors.GetOrAdd(error.Topic, _ => new List<(DeliveryError, object?)>());
        lock (list)
        {
            list.Add((error, message));
            if (list.Count > _maxErrorsPerTopic)
                list.RemoveAt(0);
        }
    }

    public IReadOnlyList<(DeliveryError Error, object? Message)> GetErrors(string topic)
    {
        if (!_errors.TryGetValue(topic, out var list))
            return Array.Empty<(DeliveryError, object?)>();
        lock (list)
        {
            return list.ToArray();
        }
    }

    public void Clear(string topic)
    {
        _errors.TryRemove(topic, out _);
    }
}
