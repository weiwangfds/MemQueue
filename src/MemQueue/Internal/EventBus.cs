// src/MemQueue/Internal/EventBus.cs
using System.Collections.Concurrent;
using MemQueue.Abstractions;
using MemQueue.Models;

namespace MemQueue.Internal;

internal sealed class EventBus
{
    private readonly ConcurrentDictionary<(TopicId Topic, Type MessageType), List<Delegate>> _handlers = new();

    internal event Action<string, Exception, Type>? HandlerError;

    public void Register<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler) where TMessage : MessageBase
    {
        var key = (topic, typeof(TMessage));
        var handlers = _handlers.GetOrAdd(key, _ => new List<Delegate>());
        lock (handlers)
        {
            handlers.Add(handler);
        }
    }

    public bool Unregister<TMessage>(
        TopicId topic,
        Func<TMessage, MessageContext, CancellationToken, ValueTask> handler) where TMessage : MessageBase
    {
        var key = (topic, typeof(TMessage));
        if (!_handlers.TryGetValue(key, out var handlers))
            return false;

        lock (handlers)
        {
            return handlers.Remove(handler);
        }
    }

    public async ValueTask FireAsync<TMessage>(
        TopicId topic,
        MessageEnvelope<TMessage> envelope,
        CancellationToken cancellationToken) where TMessage : MessageBase
    {
        if (!_handlers.TryGetValue((topic, typeof(TMessage)), out var handlers))
            return;

        Delegate[] snapshot;
        lock (handlers)
        {
            snapshot = handlers.ToArray();
        }

        var context = new MessageContext
        {
            Topic = envelope.Topic,
            Partition = envelope.Partition,
            Offset = envelope.Offset,
            Key = envelope.Key,
            Timestamp = envelope.Timestamp
        };

        var tasks = new List<ValueTask>(snapshot.Length);
        foreach (var del in snapshot)
        {
            var h = (Func<TMessage, MessageContext, CancellationToken, ValueTask>)del;
            try
            {
                tasks.Add(h(envelope.Value, context, cancellationToken));
            }
            catch (Exception ex)
            {
                FireHandlerErrorSafe(topic, ex, typeof(TMessage));
            }
        }

        for (var i = 0; i < tasks.Count; i++)
        {
            try
            {
                await tasks[i];
            }
            catch (Exception ex)
            {
                FireHandlerErrorSafe(topic, ex, typeof(TMessage));
            }
        }
    }

    private void FireHandlerErrorSafe(TopicId topic, Exception ex, Type messageType)
    {
        try { HandlerError?.Invoke(topic, ex, messageType); }
        catch { /* isolate user error handler failures */ }
    }

    internal void SetErrorHandler(Action<string, Exception, Type> handler)
    {
        HandlerError += handler;
    }
}
