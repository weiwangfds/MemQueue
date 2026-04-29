# MemQueue

[![NuGet](https://img.shields.io/nuget/v/MemQueue.svg?label=NuGet)](https://www.nuget.org/packages/MemQueue/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/MemQueue.svg)](https://www.nuget.org/packages/MemQueue/)
[![CI](https://github.com/weiwangfds/MemQueue/actions/workflows/ci.yml/badge.svg)](https://github.com/weiwangfds/MemQueue/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**In-memory Kafka-like message queue and event bus for .NET 8.** Zero external infrastructure. Process-local pub/sub with topic partitions, consumer groups, offset tracking, retention, and backpressure.

> **MemQueue** brings the Kafka programming model (topics, partitions, consumer groups, offsets) into your .NET process — no broker, no disk, no network. Ideal for in-process event sourcing, work distribution, and domain events.

## Quick Links

- [Installation](#installation)
- [Minimal Example](#minimal-example)
- [Features](#features)
- [Configuration Reference](#configuration-reference)
- [Source Generator](#source-generator)
- [FAQ](#faq)
- [Comparison](#comparison)
- [Architecture](#architecture)
- [Running Tests & Benchmarks](#running-tests--benchmarks)
- [License](#license)

## Installation

### .NET CLI

```bash
dotnet add package MemQueue
dotnet add package MemQueue.SourceGenerators
```

### PackageReference

```xml
<PackageReference Include="MemQueue" Version="1.0.2" />
<PackageReference Include="MemQueue.SourceGenerators" Version="1.0.2" />
```

### Package Overview

| Package | Description |
|---|---|
| `MemQueue` | Core library: message bus, topics, partitions, consumer groups, backpressure |
| `MemQueue.SourceGenerators` | Roslyn source generator for `[Subscribe]` attribute — auto-generates `IHostedService` boilerplate |

## Minimal Example

A complete produce/consume loop in under 50 lines:

```csharp
using MemQueue.Abstractions;
using MemQueue.DependencyInjection;
using MemQueue.Models;
using Microsoft.Extensions.DependencyInjection;

// 1. Define a message
public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;

// 2. Setup
var services = new ServiceCollection();
services.AddMemQueue(builder => builder
    .AddTopic("orders", topic => topic
        .WithPartitions(2)
        .WithBufferCapacity(1_000)));
await using var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IMessageBus>();

// 3. Produce
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Widget", 3));

// 4. Subscribe (consumer group)
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
await bus.SubscribeAsync<OrderCreated>(
    (TopicId)"orders",
    (ConsumerGroupId)"processors",
    async (msg, ctx, ct) =>
    {
        Console.WriteLine($"Processed: {msg.Product} x{msg.Quantity} [P{ctx.Partition} @O{ctx.Offset}]");
        await ValueTask.CompletedTask;
    },
    cts.Token);
```

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

## Configuration Reference

### Topic Options

```csharp
services.AddMemQueue(builder => builder
    .AddTopic("my-topic", topic => topic
        .WithPartitions(8)                           // partition count (default: 1)
        .WithBufferCapacity(10_000)                  // ring buffer size per partition (default: 1024)
        .WithRetention(RetentionPolicy.ByCount.Of(5000))
        .WithOverflowPolicy(OverflowPolicy.Block)    // OverwriteOldest | Block | DropNewest
        .WithBackpressure(bp => {
            bp.HighWatermarkPercent = 0.8;            // alert at 80% full
            bp.LowWatermarkPercent = 0.3;             // recover at 30%
            bp.SlowConsumerStrategy = SlowConsumerStrategy.Warn;
            bp.SlowConsumerLagThreshold = 500;
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

## FAQ

### What delivery guarantees does MemQueue provide?

Two modes:
- **`IMessageBus`** — At-least-once delivery. Messages persist in the ring buffer until consumed and committed. Suitable for task queues, event sourcing, and work distribution.
- **`IDomainEventBus`** — At-most-once delivery. Fire-and-forget in-process events. If a handler throws, the event is lost. Suitable for loose coupling and notifications.

### Must all messages inherit from `MessageBase`?

Yes. `MessageBase` is an abstract record that all messages must extend. This allows MemQueue to track metadata (timestamp, key) uniformly. Define messages as `public record MyMessage(...) : MessageBase;`.

### How do subscriptions work with cancellation?

`SubscribeAsync` accepts a `CancellationToken`. When cancelled, the background consumption loop stops gracefully. For graceful shutdown in ASP.NET Core, use `IHostApplicationLifetime.ApplicationStopping`.

### Should I use auto-commit or manual commit?

- **Auto-commit (`true`)** — Recommended for idempotent handlers. Offset is committed after the handler returns successfully.
- **Manual commit (`false`)** — Use when you need to commit after downstream side effects (e.g., database write). Call `await ctx.CommitAsync(ct)` when ready.

### How do I choose an overflow policy?

- **`OverwriteOldest`** (default) — Best for metrics/logs where data freshness matters more than completeness.
- **`Block`** — Use when zero data loss is required. Producers wait until consumers free space.
- **`DropNewest`** — Use when you prefer to reject new work rather than lose old work.

### Is MemQueue thread-safe?

Yes. All core components use appropriate synchronization: `Interlocked` for offset counters, `SemaphoreSlim` for blocking writes, `ReaderWriterLockSlim` for consumer group mutations, and `ConcurrentDictionary` for registries.

### Can I replay messages?

Yes. Use `bus.CreateConsumer<T>(topic)` to create a manual consumer, then call `SeekToBeginningAsync`, `SeekToEndAsync`, or `SeekAsync(partition, offset)` to position the cursor.

## Comparison

### MemQueue vs System.Threading.Channels

| Feature | MemQueue | Channels |
|---|---|---|
| Topic/Partition model | ✅ Kafka-style | ❌ Single channel |
| Consumer groups | ✅ With rebalance | ❌ Single reader |
| Offset tracking & seek | ✅ | ❌ |
| Retention policies | ✅ By count/age | ❌ Unbounded or Bounded |
| Backpressure monitoring | ✅ Watermarks | ❌ |
| Dead letter queue | ✅ | ❌ |
| Message ordering | ✅ Per-key/per-partition | ✅ FIFO only |
| Source generator | ✅ `[Subscribe]` | ❌ |

**When to use Channels**: Simple producer/consumer with bounded capacity. Lower overhead for basic scenarios.

**When to use MemQueue**: Kafka-style semantics (consumer groups, offset management, partitioning) in-process.

### MemQueue vs Apache Kafka

| Feature | MemQueue | Kafka |
|---|---|---|
| Deployment | In-process (NuGet) | External broker cluster |
| Persistence | In-memory (ring buffer) | Disk (log segments) |
| Consumer groups | ✅ | ✅ |
| Offset management | ✅ | ✅ |
| Partitions | ✅ | ✅ |
| Backpressure | ✅ Watermark events | ✅ Via quota/fetch config |
| Cross-process | ❌ Process-local only | ✅ Network-accessible |
| Latency | Sub-microsecond | Milliseconds (network) |

**When to use Kafka**: Multi-process/multi-service communication, durable event streaming, cross-datacenter replication.

**When to use MemQueue**: Single-process scenarios where you want the Kafka programming model without infrastructure overhead.

### MemQueue vs MediatR / In-process event bus

| Feature | MemQueue | MediatR |
|---|---|---|
| Pattern | Message queue (pub/sub with buffering) | Mediator (request/response + pub/sub) |
| Consumer groups | ✅ Load distribution | ❌ All handlers receive |
| Partitioning | ✅ | ❌ |
| Offset/replay | ✅ | ❌ |
| Backpressure | ✅ | ❌ |
| Ordering | ✅ Per-key/per-partition | ❌ |
| Request/response | ❌ | ✅ IRequest/IRequestHandler |
| Dead letter queue | ✅ | ❌ |

**When to use MediatR**: CQRS patterns, request/response, decoupling request handling from controllers.

**When to use MemQueue**: High-throughput async message processing, event sourcing, work queues with consumer group semantics.

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
| `Bus` | Main facade. Implements `IMessageBus` (at-least-once) and `IDomainEventBus` (at-most-once). |
| `BackpressureCoordinator` | Tracks per-partition watermarks. Fires watermark and slow consumer events. |
| `RetentionManager` | Background `PeriodicTimer` loop that applies retention policies across all topics. |

### Delivery Guarantees

| Mode | Interface | Guarantee | Use Case |
|---|---|---|---|
| Message Bus | `IMessageBus` | At-least-once | Task queues, event sourcing, work distribution |
| Domain Event Bus | `IDomainEventBus` | At-most-once | In-process notifications, loose coupling |

### Thread Safety

- `AppendOnlyLog` uses `Interlocked` for offset counters, `SemaphoreSlim` for blocking writes, and `AsyncReaderWriterLock` for retention reads
- `ConsumerGroup` uses `ReaderWriterLockSlim` for member/assignment mutations
- `GroupCoordinator` uses `ConcurrentDictionary` for group registry
- `TopicManager` uses `ConcurrentDictionary` for topic registry

### Performance Characteristics

- **Zero-allocation partitioning** — FNV-1a hash for key routing (no SHA256 allocation)
- **Lock-free offset counters** — `Interlocked.Increment` for head offset
- **Async notification** — `TaskCompletionSource`-based pulse (no polling)
- **Ring buffer** — Fixed-size `StoredEntry?[]` with modular indexing, no resizing
- **Partition fan-out** — `Task.WhenAny` across all assigned partitions for fast consume

## Backpressure Monitoring

```csharp
var monitor = bus.BackpressureMonitor;

monitor.HighWatermarkReached += (topic, partition, utilization) =>
    Console.WriteLine($"WARNING: {topic}/{partition} at {utilization:P0}");

monitor.LowWatermarkRecovered += (topic, partition, utilization) =>
    Console.WriteLine($"RECOVERED: {topic}/{partition} at {utilization:P0}");

monitor.SlowConsumerDetected += (topic, partition, groupId, lag) =>
    Console.WriteLine($"SLOW CONSUMER: {groupId} on {topic}/{partition} lag={lag}");

var util = monitor.GetUtilization("orders", 0);
var lag = monitor.GetConsumerLag("orders", 0);
var blocked = monitor.GetBlockedProducerCount("orders", 0);
```

## Dead Letter Queue

```csharp
var dlq = new InMemoryDeadLetterQueue(maxErrorsPerTopic: 100);
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

Console.WriteLine($"Topic: {stats.Topic} | Partitions: {stats.PartitionCount} | Messages: {stats.TotalMessages}");

foreach (var (pid, p) in stats.Partitions)
{
    Console.WriteLine($"  P{pid}: head={p.HeadOffset} tail={p.TailOffset} count={p.MessageCount} util={p.Utilization:P1}");
}
```

## Running Tests & Benchmarks

```bash
# Run tests
dotnet test tests/MemQueue.Tests/

# Run benchmarks
dotnet run --project tests/MemQueue.Benchmarks/
```

## License

This project is licensed under the [MIT License](LICENSE).
