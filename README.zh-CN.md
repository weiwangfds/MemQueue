# MemQueue

[![NuGet](https://img.shields.io/nuget/v/MemQueue.svg?label=NuGet)](https://www.nuget.org/packages/MemQueue/)
[![NuGet Downloads](https://img.shields.io/nuget/dt/MemQueue.svg)](https://www.nuget.org/packages/MemQueue/)
[![CI](https://github.com/weiwangfds/MemQueue/actions/workflows/ci.yml/badge.svg)](https://github.com/weiwangfds/MemQueue/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**面向 .NET 8 的内存级 Kafka 风格消息队列与事件总线。** 零外部基础设施依赖，进程内发布/订阅，支持 Topic 分区、消费者组、Offset 追踪、消息保留与背压控制。

> **MemQueue** 将 Kafka 编程模型（Topic、Partition、Consumer Group、Offset）引入 .NET 进程内 —— 无需 Broker、无需磁盘、无需网络。适用于进程内事件溯源、工作分发和领域事件。

## 快速导航

- [安装](#安装)
- [最小示例](#最小示例)
- [功能特性](#功能特性)
- [配置参考](#配置参考)
- [Source Generator](#source-generator)
- [常见问题](#常见问题)
- [对比](#对比)
- [架构](#架构)
- [测试与基准](#测试与基准)
- [许可证](#许可证)

## 安装

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

### 包概览

| 包 | 说明 |
|---|---|
| `MemQueue` | 核心库：消息总线、Topic、分区、消费者组、背压控制 |
| `MemQueue.SourceGenerators` | Roslyn 源码生成器，`[Subscribe]` 特性自动生成 `IHostedService` 样板代码 |

## 最小示例

不到 50 行的完整发布/消费闭环：

```csharp
using MemQueue.Abstractions;
using MemQueue.DependencyInjection;
using MemQueue.Models;
using Microsoft.Extensions.DependencyInjection;

// 1. 定义消息
public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;

// 2. 配置
var services = new ServiceCollection();
services.AddMemQueue(builder => builder
    .AddTopic("orders", topic => topic
        .WithPartitions(2)
        .WithBufferCapacity(1_000)));
await using var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IMessageBus>();

// 3. 发布
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Widget", 3));

// 4. 订阅（消费者组）
using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
await bus.SubscribeAsync<OrderCreated>(
    (TopicId)"orders",
    (ConsumerGroupId)"processors",
    async (msg, ctx, ct) =>
    {
        Console.WriteLine($"已处理：{msg.Product} x{msg.Quantity} [P{ctx.Partition} @O{ctx.Offset}]");
        await ValueTask.CompletedTask;
    },
    cts.Token);
```

## 功能特性

- **Topic 与分区** — Kafka 风格的 Topic/Partition 模型，可配置分区数
- **消费者组** — 多消费者分摊负载，通过 Rebalance 策略自动分配分区
- **Offset 管理** — 手动提交、自动提交、Seek（起始/末尾/任意位置）
- **消息保留策略** — 按消息数量、存活时间或两者组合裁剪，后台定时清理
- **溢出控制** — 覆盖最旧、阻塞等待（背压）、拒绝新消息三种策略
- **背压监控** — 高/低水位线事件、慢消费者检测、生产者阻塞计数
- **死信队列** — 捕获处理失败的消息及错误元数据
- **领域事件总线** — 轻量级即发即弃的进程内事件机制（最多一次语义）
- **顺序模式** — 无序、分区内有序、按 Key 有序（FNV-1a 哈希路由）
- **Source Generator** — `[Subscribe]` 特性自动生成 `IHostedService` 订阅代码
- **DI 集成** — `AddMemQueue()` Builder 模式，流式配置 Topic
- **Topic 统计** — 分区级别的 Head/Tail Offset、消息数量、缓冲区利用率

## 配置参考

### Topic 选项

```csharp
services.AddMemQueue(builder => builder
    .AddTopic("my-topic", topic => topic
        .WithPartitions(8)                           // 分区数（默认: 1）
        .WithBufferCapacity(10_000)                  // 每分区环形缓冲区大小（默认: 1024）
        .WithRetention(RetentionPolicy.ByCount.Of(5000))
        .WithOverflowPolicy(OverflowPolicy.Block)    // OverwriteOldest | Block | DropNewest
        .WithBackpressure(bp => {
            bp.HighWatermarkPercent = 0.8;            // 80% 时触发高水位告警
            bp.LowWatermarkPercent = 0.3;             // 30% 时触发低水位恢复
            bp.SlowConsumerStrategy = SlowConsumerStrategy.Warn;
            bp.SlowConsumerLagThreshold = 500;
        })
        .WithOrdering(OrderingMode.PerKey)           // None | PerPartition | PerKey
    )
);
```

### 全局选项

```csharp
services.AddMemQueue(builder => builder
    .SetDefaultOrdering(OrderingMode.PerPartition)
    .UseKeyHashPartitioner()    // 或 UseRoundRobinPartitioner()（默认）
    .UseRangeRebalancer()       // 或 UseRoundRobinRebalancer()
);
```

### 消费者组选项

| 选项 | 默认值 | 说明 |
|---|---|---|
| `AutoCommit` | `true` | 处理成功后自动提交 Offset |
| `AutoOffsetReset` | `Latest` | 无已提交 Offset 时的起始位置（`Latest`、`Earliest`、`Error`） |

### 溢出策略

| 策略 | 行为 |
|---|---|
| `OverwriteOldest` | 缓冲区满时覆盖最旧条目（默认） |
| `Block` | 阻塞生产者直到有空闲空间，零数据丢失 |
| `DropNewest` | 缓冲区满时拒绝新消息，抛出 `BufferOverflowException` |

### 消息保留策略

| 策略 | 说明 |
|---|---|
| `RetentionPolicy.ByCount.Of(n)` | 保留最近 `n` 条消息 |
| `RetentionPolicy.ByAge.Of(t)` | 保留最近 `t` 时间段内的消息 |
| `new RetentionPolicy.ByCountOrAge(n, t)` | 同时按数量和时间裁剪 |
| `RetentionPolicy.None` | 无限期保留所有消息 |

## Source Generator

`MemQueue.SourceGenerators` 包提供 `[Subscribe]` 特性，实现声明式订阅注册。

```csharp
using MemQueue;
using MemQueue.Abstractions;
using MemQueue.Models;

[Subscribe("orders", GroupId = "order-processors", AutoCommit = true)]
public class OrderHandler : IMessageHandler<OrderCreated>
{
    public async ValueTask HandleAsync(OrderCreated message, MessageContext context, CancellationToken ct)
    {
        Console.WriteLine($"处理订单 {message.OrderId}");
        await ValueTask.CompletedTask;
    }
}
```

注册所有带 `[Subscribe]` 标记的处理器：

```csharp
services.AddMemQueueSubscribers();
```

Source Generator 将自动生成：
- `AddMemQueueSubscribers()` 扩展方法
- 每个订阅者类对应的 `IHostedService` 实现
- Scoped DI 解析每个 Handler

## 常见问题

### MemQueue 提供什么投递保证？

两种模式：
- **`IMessageBus`** — 至少一次投递。消息持久化在环形缓冲区中，直到被消费并提交。适用于任务队列、事件溯源和工作分发。
- **`IDomainEventBus`** — 最多一次投递。即发即弃的进程内事件。如果处理器抛出异常，事件将丢失。适用于松耦合和通知场景。

### 所有消息都必须继承 MessageBase 吗？

是的。`MessageBase` 是所有消息必须继承的抽象 record。这使 MemQueue 能够统一追踪元数据（时间戳、Key）。消息定义方式：`public record MyMessage(...) : MessageBase;`。

### 订阅如何配合取消？

`SubscribeAsync` 接受 `CancellationToken`。取消时，后台消费循环会优雅停止。在 ASP.NET Core 中可使用 `IHostApplicationLifetime.ApplicationStopping` 实现优雅关机。

### 应该用自动提交还是手动提交？

- **自动提交 (`true`)** — 适用于幂等处理器。处理器成功返回后自动提交 Offset。
- **手动提交 (`false`)** — 适用于需要在下游副作用完成后再提交的场景（如数据库写入）。调用 `await ctx.CommitAsync(ct)` 即可。

### 如何选择溢出策略？

- **`OverwriteOldest`**（默认） — 适用于指标/日志等数据新鲜度比完整性更重要的场景。
- **`Block`** — 要求零数据丢失时使用。生产者等待消费者释放空间。
- **`DropNewest`** — 优先拒绝新工作而非丢失旧工作时使用。

### MemQueue 是线程安全的吗？

是的。所有核心组件使用适当的同步机制：`Interlocked` 管理 Offset 计数器、`SemaphoreSlim` 处理阻塞写入、`ReaderWriterLockSlim` 保护消费者组变更、`ConcurrentDictionary` 管理注册表。

### 可以重放消息吗？

可以。使用 `bus.CreateConsumer<T>(topic)` 创建手动消费者，然后调用 `SeekToBeginningAsync`、`SeekToEndAsync` 或 `SeekAsync(partition, offset)` 定位游标。

## 对比

### MemQueue vs System.Threading.Channels

| 特性 | MemQueue | Channels |
|---|---|---|
| Topic/Partition 模型 | ✅ Kafka 风格 | ❌ 单一通道 |
| 消费者组 | ✅ 支持 Rebalance | ❌ 单一读取者 |
| Offset 追踪与 Seek | ✅ | ❌ |
| 消息保留策略 | ✅ 按数量/时间 | ❌ 无界或有界 |
| 背压监控 | ✅ 水位线 | ❌ |
| 死信队列 | ✅ | ❌ |
| 消息顺序 | ✅ 按 Key/分区 | ✅ 仅 FIFO |
| Source Generator | ✅ `[Subscribe]` | ❌ |

**何时用 Channels**：有界容量的简单生产者/消费者，基础场景开销更低。

**何时用 MemQueue**：进程内需要 Kafka 语义（消费者组、Offset 管理、分区）。

### MemQueue vs Apache Kafka

| 特性 | MemQueue | Kafka |
|---|---|---|
| 部署方式 | 进程内（NuGet） | 外部 Broker 集群 |
| 持久化 | 内存（环形缓冲区） | 磁盘（日志段） |
| 消费者组 | ✅ | ✅ |
| Offset 管理 | ✅ | ✅ |
| 分区 | ✅ | ✅ |
| 背压 | ✅ 水位线事件 | ✅ 通过配额/拉取配置 |
| 跨进程 | ❌ 仅进程内 | ✅ 网络可达 |
| 延迟 | 亚微秒 | 毫秒级（网络） |

**何时用 Kafka**：多进程/多服务通信、持久事件流、跨数据中心复制。

**何时用 MemQueue**：单进程场景，需要 Kafka 编程模型但不需要基础设施开销。

### MemQueue vs MediatR / 进程内事件总线

| 特性 | MemQueue | MediatR |
|---|---|---|
| 模式 | 消息队列（带缓冲的发布/订阅） | 中介者（请求/响应 + 发布/订阅） |
| 消费者组 | ✅ 负载分配 | ❌ 所有处理器都收到 |
| 分区 | ✅ | ❌ |
| Offset/重放 | ✅ | ❌ |
| 背压 | ✅ | ❌ |
| 顺序保证 | ✅ 按 Key/分区 | ❌ |
| 请求/响应 | ❌ | ✅ IRequest/IRequestHandler |
| 死信队列 | ✅ | ❌ |

**何时用 MediatR**：CQRS 模式、请求/响应、解耦控制器与请求处理。

**何时用 MemQueue**：高吞吐异步消息处理、事件溯源、带消费者组语义的工作队列。

## 架构

```
┌─────────────────────────────────────────────────────┐
│                        Bus                          │
│         (IMessageBus + IDomainEventBus)              │
├──────────┬──────────┬───────────┬───────────────────┤
│Producer<T│Consumer<T│订阅管理器  │     EventBus      │
│    >     │    >     │(SubMgr)   │  (即发即弃事件)    │
├──────────┴──────────┴───────────┴───────────────────┤
│                  ClientFactory                      │
├─────────────────────────────────────────────────────┤
│               GroupCoordinator                      │
│        (消费者组注册，Offset 提交)                    │
├─────────────────────────────────────────────────────┤
│                  TopicManager                       │
│       (Topic/Partition 生命周期，存储查找)            │
├─────────────────────────────────────────────────────┤
│  PartitionLog<T>  │  PartitionLog<T>  │  ...        │
│  (类型化门面)      │  (类型化门面)      │             │
├───────────────────┼───────────────────┤             │
│  AppendOnlyLog    │  AppendOnlyLog    │  ...        │
│  (环形缓冲区)      │  (环形缓冲区)      │             │
└───────────────────┴───────────────────┴─────────────┘
         │                           │
  BackpressureCoordinator    RetentionManager
  (水位线，慢消费者)           (后台裁剪)
```

### 核心组件

| 组件 | 职责 |
|---|---|
| `AppendOnlyLog` | 无锁环形缓冲区，管理 Head/Tail Offset。处理溢出策略、保留裁剪和异步通知。 |
| `PartitionLog<T>` | `AppendOnlyLog` 的类型化门面，将原始 `StoredEntry` 映射为 `MessageEnvelope<T>`。 |
| `TopicManager` | 管理 Topic/Partition 生命周期，为每个分区创建 `AppendOnlyLog` 实例。 |
| `GroupCoordinator` | 消费者组全局注册中心，路由 Offset 提交和分区分配。 |
| `Bus` | 主入口门面，实现 `IMessageBus`（至少一次）和 `IDomainEventBus`（最多一次）。 |
| `BackpressureCoordinator` | 追踪分区水位线，触发水位线和慢消费者事件。 |
| `RetentionManager` | 后台 `PeriodicTimer` 循环，对所有 Topic 应用保留策略。 |

### 投递保证

| 模式 | 接口 | 保证 | 适用场景 |
|---|---|---|---|
| 消息总线 | `IMessageBus` | 至少一次 | 任务队列、事件溯源、工作分发 |
| 领域事件总线 | `IDomainEventBus` | 最多一次 | 进程内通知、松耦合 |

### 线程安全

- `AppendOnlyLog` 使用 `Interlocked` 管理 Offset 计数器、`SemaphoreSlim` 处理阻塞写入、`AsyncReaderWriterLock` 保护保留读操作
- `ConsumerGroup` 使用 `ReaderWriterLockSlim` 保护成员/分配变更
- `GroupCoordinator` 使用 `ConcurrentDictionary` 管理组注册表
- `TopicManager` 使用 `ConcurrentDictionary` 管理 Topic 注册表

### 性能特征

- **零分配分区选择** — FNV-1a 哈希进行 Key 路由（无 SHA256 内存分配）
- **无锁 Offset 计数器** — `Interlocked.Increment` 自增 Head Offset
- **异步通知机制** — 基于 `TaskCompletionSource` 的脉冲信号，无需轮询
- **环形缓冲区** — 固定大小 `StoredEntry?[]`，取模索引，无扩容开销
- **分区扇出消费** — `Task.WhenAny` 监听所有已分配分区，首个响应立即返回

## 测试与基准

```bash
# 运行测试
dotnet test tests/MemQueue.Tests/

# 运行基准测试
dotnet run --project tests/MemQueue.Benchmarks/
```

## 许可证

本项目基于 [MIT 许可证](LICENSE) 授权。
