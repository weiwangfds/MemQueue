# MemQueue

In-memory Kafka-like message queue and event bus for .NET 8. Zero external infrastructure. Process-local pub/sub with topic partitions, consumer groups, offset tracking, retention, and backpressure.

## Features

- **Topics & Partitions** — Kafka-style topic/partition model with configurable partition count
- **Consumer Groups** — Multiple consumers share load; partitions assigned via rebalance strategies
- **Offset Management** — Manual commit, auto-commit, seek (beginning/end/arbitrary)
- **Retention Policies** — Trim by message count, age, or both; background cleanup via `PeriodicTimer`
- **Overflow Control** — Overwrite oldest, block with backpressure, or reject (drop newest)
- **Backpressure Monitoring** — High/low watermark events, slow consumer detection, producer block count
- **Dead Letter Queue** — Capture failed message deliveries with error metadata
- **Domain Event Bus** — Lightweight fire-and-forget in-process events (at-most-once)
- **Ordering Modes** — None, per-partition, or per-key (key-hash routing via FNV-1a)
- **Source Generator** — `[Subscribe]` attribute generates `IHostedService` boilerplate automatically
- **DI Integration** — `AddMemQueue()` builder with fluent topic configuration
- **Topic Statistics** — Per-partition head/tail offsets, message count, buffer utilization

## Installation

```xml
<PackageReference Include="MemQueue" Version="1.0.0" />
<PackageReference Include="MemQueue.SourceGenerators" Version="1.0.0" />
```

## Quick Start

### 1. Define Messages

All messages inherit from `MessageBase` (abstract record):

```csharp
using MemQueue.Abstractions;

public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;
public record OrderShipped(Guid OrderId, string TrackingNumber) : MessageBase;
```

### 2. Register with DI

```csharp
using MemQueue.DependencyInjection;
using MemQueue.Models;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

services.AddMemQueue(builder => builder
    .AddTopic("orders", topic => topic
        .WithPartitions(4)
        .WithBufferCapacity(10_000)
        .WithRetention(RetentionPolicy.ByCount.Of(10_000)))
    .AddTopic("shipping", topic => topic
        .WithPartitions(2))
    .UseKeyHashPartitioner()       // optional: default is RoundRobin
    .UseRangeRebalancer()          // optional: default is Range
);

var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IMessageBus>();
```

### 3. Publish Messages

```csharp
// One-off produce via bus
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Widget", 3));

// Produce with key (routes to consistent partition when OrderingMode.PerKey is set)
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Gadget", 1), key: "user-42");

// Produce to specific partition
await bus.ProduceAsync((TopicId)"orders", new PartitionId(0), new OrderCreated(Guid.NewGuid(), "Thingy", 5));

// Or create a typed producer for repeated use
var producer = bus.CreateProducer<OrderCreated>((TopicId)"orders");
var result = await producer.ProduceAsync(new OrderCreated(Guid.NewGuid(), "Widget", 3));
Console.WriteLine($"Produced to partition {result.Partition} at offset {result.Offset}");

// Batch produce
await producer.ProduceBatchAsync([
    new OrderCreated(Guid.NewGuid(), "A", 1),
    new OrderCreated(Guid.NewGuid(), "B", 2),
]);
```

### 4. Subscribe (Broadcast)

Every subscriber receives every message. No consumer group needed.

```csharp
using var cts = new CancellationTokenSource();

await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", async (msg, ctx, ct) =>
{
    Console.WriteLine($"Order {msg.OrderId}: {msg.Quantity}x {msg.Product}");
    Console.WriteLine($"  partition={ctx.Partition}, offset={ctx.Offset}");
    await ValueTask.CompletedTask;
}, cts.Token);
```

### 5. Subscribe (Consumer Group)

Messages are distributed across group members. Each message is delivered to exactly one consumer in the group.

```csharp
var groupId = (ConsumerGroupId)"order-processors";

// Consumer 1 — auto-commit enabled
await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", groupId, async (msg, ctx, ct) =>
{
    Console.WriteLine($"[Processor-1] Processing {msg.OrderId}");
    await ctx.CommitAsync(ct);  // optional when autoCommit: true
}, cts.Token);

// Consumer 2 — manual commit
await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", groupId, autoCommit: false, async (msg, ctx, ct) =>
{
    Console.WriteLine($"[Processor-2] Processing {msg.OrderId}");
    await ctx.CommitAsync(ct);  // required
}, cts.Token);
```

### 6. Manual Consumer (Replay / Seek)

```csharp
var consumer = bus.CreateConsumer<OrderCreated>((TopicId)"orders");

// Seek to beginning of partition 0
await consumer.SeekToBeginningAsync(new PartitionId(0));
var first = await consumer.ConsumeAsync();

// Stream all available messages
await foreach (var result in consumer.ConsumeAllAsync(cts.Token))
{
    Console.WriteLine($"Consumed: {result.Value.Product} from partition {result.Partition}");
    await consumer.CommitAsync(result.Offset, result.Partition);
}

// Seek to end (only new messages)
await consumer.SeekToEndAsync(new PartitionId(0));

// Seek to specific offset
await consumer.SeekAsync(new PartitionId(0), new Offset(42));
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                        Bus                          │
│         (IMessageBus + IDomainEventBus)              │
├──────────┬──────────┬───────────┬───────────────────┤
│Producer<T│Consumer<T│ Subscripti│     EventBus      │
│    >     │    >     │onManager  │  (fire-and-forget)│
├──────────┴──────────┴───────────┴───────────────────┤
│                  ClientFactory                      │
├─────────────────────────────────────────────────────┤
│               GroupCoordinator                      │
│    (consumer group registry, offset commits)        │
├─────────────────────────────────────────────────────┤
│                  TopicManager                       │
│    (topic/partition lifecycle, store lookups)       │
├─────────────────────────────────────────────────────┤
│  PartitionLog<T>  │  PartitionLog<T>  │  ...        │
│  (typed facade)   │  (typed facade)   │             │
├───────────────────┼───────────────────┤             │
│  AppendOnlyLog    │  AppendOnlyLog    │  ...        │
│  (ring buffer)    │  (ring buffer)    │             │
└───────────────────┴───────────────────┴─────────────┘
         │                           │
  BackpressureCoordinator    RetentionManager
  (watermarks, slow consumer)  (background trim)
```

### Core Components

| Component | Responsibility |
|---|---|
| `AppendOnlyLog` | Lock-free ring buffer with head/tail offsets. Handles overflow policies, retention trimming, and async notification. |
| `PartitionLog<T>` | Typed facade over `AppendOnlyLog`. Projects raw `StoredEntry` into `MessageEnvelope<T>`. |
| `TopicManager` | Manages topic/partition lifecycle. Creates `AppendOnlyLog` instances per partition. |
| `GroupCoordinator` | Global registry of consumer groups. Routes offset commits and partition assignments. |
| `ConsumerGroup` | Per-topic group with `ReaderWriterLockSlim`-protected member list and partition assignments. |
| `Bus` | Main facade. Implements `IMessageBus` (at-least-once) and `IDomainEventBus` (at-most-once). |
| `SubscriptionManager` | Runs background `Task.Run` loops for each `SubscribeAsync` call. Handles shutdown lifecycle. |
| `BackpressureCoordinator` | Tracks per-partition watermarks. Fires `HighWatermarkReached` / `LowWatermarkRecovered` / `SlowConsumerDetected` events. |
| `RetentionManager` | Background `PeriodicTimer` loop that applies retention policies across all topics. |
| `MessageNotifier` | Async pulse signal using `TaskCompletionSource`. Equivalent to `Monitor.PulseAll()` for async. |

## Configuration Reference

### Topic Options

```csharp
services.AddMemQueue(builder => builder
    .AddTopic("my-topic", topic => topic
        .WithPartitions(8)                           // partition count (default: 1)
        .WithBufferCapacity(10_000)                  // ring buffer size per partition (default: 1024)
        .WithRetention(RetentionPolicy.ByCount.Of(5000))           // retain last N messages
        // .WithRetention(RetentionPolicy.ByAge.Of(TimeSpan.FromHours(1)))  // retain by age
        // .WithRetention(new RetentionPolicy.ByCountOrAge(5000, TimeSpan.FromHours(1)))
        .WithOverflowPolicy(OverflowPolicy.Block)    // OverwriteOldest | Block | DropNewest
        .WithBackpressure(bp => {
            bp.HighWatermarkPercent = 0.8;            // alert at 80% full
            bp.LowWatermarkPercent = 0.3;             // recover at 30%
            bp.SlowConsumerStrategy = SlowConsumerStrategy.Warn;  // None | Warn | ForceAdvance
            bp.SlowConsumerLagThreshold = 500;        // lag threshold
        })
        .WithOrdering(OrderingMode.PerKey)           // None | PerPartition | PerKey
    )
);
```

### Global Options

```csharp
services.AddMemQueue(builder => builder
    .SetDefaultOrdering(OrderingMode.PerPartition)
    .UseKeyHashPartitioner()    // or UseRoundRobinPartitioner() (default)
    .UseRangeRebalancer()       // or UseRoundRobinRebalancer()
);
```

### Consumer Group Options

| Option | Default | Description |
|---|---|---|
| `AutoCommit` | `true` | Automatically commit offset after handler succeeds |
| `AutoOffsetReset` | `Latest` | Starting offset when no committed offset exists (`Latest`, `Earliest`, `Error`) |

### Overflow Policies

| Policy | Behavior |
|---|---|
| `OverwriteOldest` | Overwrites oldest entries when buffer is full. Default. |
| `Block` | Blocks the producer until space is freed. Zero data loss. |
| `DropNewest` | Rejects new messages with `BufferOverflowException`. |

### Retention Policies

| Policy | Description |
|---|---|
| `RetentionPolicy.ByCount.Of(n)` | Keep last `n` messages |
| `RetentionPolicy.ByAge.Of(t)` | Keep messages newer than `t` |
| `new RetentionPolicy.ByCountOrAge(n, t)` | Trim by both count and age |
| `RetentionPolicy.None` | Retain all messages indefinitely |

## Source Generator

The `MemQueue.SourceGenerators` package provides the `[Subscribe]` attribute for declarative subscription registration.

### Usage

```csharp
using MemQueue;
using MemQueue.Abstractions;
using MemQueue.Models;

[Subscribe("orders", GroupId = "order-processors", AutoCommit = true)]
public class OrderHandler : IMessageHandler<OrderCreated>
{
    public async ValueTask HandleAsync(OrderCreated message, MessageContext context, CancellationToken ct)
    {
        Console.WriteLine($"Processing order {message.OrderId}");
        await ValueTask.CompletedTask;
    }
}
```

Register all `[Subscribe]`-decorated handlers:

```csharp
services.AddMemQueueSubscribers();
```

The source generator produces:
- `AddMemQueueSubscribers()` extension method
- `IHostedService` implementations per subscriber class
- Scoped DI resolution for each handler

## Backpressure Monitoring

```csharp
var monitor = bus.BackpressureMonitor;

// Wire up events
monitor.HighWatermarkReached += (topic, partition, utilization) =>
    Console.WriteLine($"WARNING: {topic}/{partition} at {utilization:P0}");

monitor.LowWatermarkRecovered += (topic, partition, utilization) =>
    Console.WriteLine($"RECOVERED: {topic}/{partition} at {utilization:P0}");

monitor.SlowConsumerDetected += (topic, partition, groupId, lag) =>
    Console.WriteLine($"SLOW CONSUMER: {groupId} on {topic}/{partition} lag={lag}");

// Poll metrics
var util = monitor.GetUtilization("orders", 0);
var lag = monitor.GetConsumerLag("orders", 0);
var blocked = monitor.GetBlockedProducerCount("orders", 0);
```

## Dead Letter Queue

```csharp
// The IDeadLetterQueue captures failed deliveries
var dlq = new InMemoryDeadLetterQueue(maxErrorsPerTopic: 100);

// Passed via SubscriptionManager on subscribe (internal API)
// Check errors:
var errors = dlq.GetErrors("orders");
foreach (var (error, message) in errors)
{
    Console.WriteLine($"Failed: {error.ErrorType} at offset {error.Offset}: {error.ErrorMessage}");
}
```

## Topic Statistics

```csharp
var topicManager = provider.GetRequiredService<ITopicManager>();
var stats = topicManager.GetTopicStatistics((TopicId)"orders");

Console.WriteLine($"Topic: {stats.Topic}");
Console.WriteLine($"Partitions: {stats.PartitionCount}");
Console.WriteLine($"Total Messages: {stats.TotalMessages}");
Console.WriteLine($"Avg Utilization: {stats.AverageUtilization:P1}");

foreach (var (pid, p) in stats.Partitions)
{
    Console.WriteLine($"  P{pid}: head={p.HeadOffset} tail={p.TailOffset} count={p.MessageCount} util={p.Utilization:P1}");
}
```

## Delivery Guarantees

| Mode | Interface | Guarantee | Use Case |
|---|---|---|---|
| Message Bus | `IMessageBus` | At-least-once | Task queues, event sourcing, work distribution |
| Domain Event Bus | `IDomainEventBus` | At-most-once | In-process notifications, loose coupling |

## Thread Safety

- `AppendOnlyLog` uses `Interlocked` for offset counters, `SemaphoreSlim` for blocking writes, and `AsyncReaderWriterLock` for retention reads
- `ConsumerGroup` uses `ReaderWriterLockSlim` for member/assignment mutations
- `GroupCoordinator` uses `ConcurrentDictionary` for group registry
- `TopicManager` uses `ConcurrentDictionary` for topic registry
- `SubscriptionManager` uses a plain `lock` for task tracking

## Performance Characteristics

- **Zero-allocation partitioning** — FNV-1a hash for key routing (no SHA256 allocation)
- **Lock-free offset counters** — `Interlocked.Increment` for head offset
- **Async notification** — `TaskCompletionSource`-based pulse (no polling)
- **Ring buffer** — Fixed-size `StoredEntry?[]` with modular indexing, no resizing
- **Partition fan-out** — `Task.WhenAny` across all assigned partitions for fast consume

## Running Tests

```bash
dotnet test tests/MemQueue.Tests/
```

## Running Benchmarks

```bash
dotnet run --project tests/MemQueue.Benchmarks/
```

## License

This project is licensed under the terms found in the repository.
