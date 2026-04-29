# MemQueue

面向 .NET 8 的内存级 Kafka 风格消息队列与事件总线。零外部基础设施依赖，进程内发布/订阅，支持 Topic 分区、消费者组、Offset 追踪、消息保留与背压控制。

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

## 安装

```xml
<PackageReference Include="MemQueue" Version="1.0.0" />
<PackageReference Include="MemQueue.SourceGenerators" Version="1.0.0" />
```

## 快速入门

### 1. 定义消息类型

所有消息继承自 `MessageBase`（抽象 record）：

```csharp
using MemQueue.Abstractions;

public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;
public record OrderShipped(Guid OrderId, string TrackingNumber) : MessageBase;
```

### 2. 注册 DI 容器

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
    .UseKeyHashPartitioner()       // 可选，默认 RoundRobin
    .UseRangeRebalancer()          // 可选，默认 Range
);

var provider = services.BuildServiceProvider();
var bus = provider.GetRequiredService<IMessageBus>();
```

### 3. 发布消息

```csharp
// 通过 Bus 直接发布单条消息
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Widget", 3));

// 带 Key 发布（OrderingMode.PerKey 时路由到固定分区）
await bus.ProduceAsync((TopicId)"orders", new OrderCreated(Guid.NewGuid(), "Gadget", 1), key: "user-42");

// 指定分区发布
await bus.ProduceAsync((TopicId)"orders", new PartitionId(0), new OrderCreated(Guid.NewGuid(), "Thingy", 5));

// 创建类型化 Producer 用于高频发布
var producer = bus.CreateProducer<OrderCreated>((TopicId)"orders");
var result = await producer.ProduceAsync(new OrderCreated(Guid.NewGuid(), "Widget", 3));
Console.WriteLine($"已发布到分区 {result.Partition}，偏移量 {result.Offset}");

// 批量发布
await producer.ProduceBatchAsync([
    new OrderCreated(Guid.NewGuid(), "A", 1),
    new OrderCreated(Guid.NewGuid(), "B", 2),
]);
```

### 4. 订阅（广播模式）

每个订阅者都会收到全量消息，无需消费者组。

```csharp
using var cts = new CancellationTokenSource();

await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", async (msg, ctx, ct) =>
{
    Console.WriteLine($"订单 {msg.OrderId}：{msg.Quantity}x {msg.Product}");
    Console.WriteLine($"  分区={ctx.Partition}，偏移量={ctx.Offset}");
    await ValueTask.CompletedTask;
}, cts.Token);
```

### 5. 订阅（消费者组模式）

消息在组内成员间分配，每条消息仅投递给组内一个消费者。

```csharp
var groupId = (ConsumerGroupId)"order-processors";

// 消费者 1 — 自动提交
await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", groupId, async (msg, ctx, ct) =>
{
    Console.WriteLine($"[处理器-1] 处理订单 {msg.OrderId}");
    await ctx.CommitAsync(ct);  // autoCommit: true 时可选
}, cts.Token);

// 消费者 2 — 手动提交
await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", groupId, autoCommit: false, async (msg, ctx, ct) =>
{
    Console.WriteLine($"[处理器-2] 处理订单 {msg.OrderId}");
    await ctx.CommitAsync(ct);  // 必须手动提交
}, cts.Token);
```

### 6. 手动消费（回放 / Seek）

```csharp
var consumer = bus.CreateConsumer<OrderCreated>((TopicId)"orders");

// Seek 到分区 0 的起始位置
await consumer.SeekToBeginningAsync(new PartitionId(0));
var first = await consumer.ConsumeAsync();

// 流式消费所有可用消息
await foreach (var result in consumer.ConsumeAllAsync(cts.Token))
{
    Console.WriteLine($"消费：{result.Value.Product}，来自分区 {result.Partition}");
    await consumer.CommitAsync(result.Offset, result.Partition);
}

// Seek 到末尾（仅消费新消息）
await consumer.SeekToEndAsync(new PartitionId(0));

// Seek 到指定 Offset
await consumer.SeekAsync(new PartitionId(0), new Offset(42));
```

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
| `ConsumerGroup` | 单个消费者组，使用 `ReaderWriterLockSlim` 保护成员列表和分区分配。 |
| `Bus` | 主入口门面，实现 `IMessageBus`（至少一次）和 `IDomainEventBus`（最多一次）。 |
| `SubscriptionManager` | 为每次 `SubscribeAsync` 启动后台 `Task.Run` 循环，管理关闭生命周期。 |
| `BackpressureCoordinator` | 追踪分区水位线，触发 `HighWatermarkReached` / `LowWatermarkRecovered` / `SlowConsumerDetected` 事件。 |
| `RetentionManager` | 后台 `PeriodicTimer` 循环，对所有 Topic 应用保留策略。 |
| `MessageNotifier` | 基于 `TaskCompletionSource` 的异步脉冲信号，等同于异步版 `Monitor.PulseAll()`。 |

## 配置参考

### Topic 选项

```csharp
services.AddMemQueue(builder => builder
    .AddTopic("my-topic", topic => topic
        .WithPartitions(8)                           // 分区数（默认: 1）
        .WithBufferCapacity(10_000)                  // 每分区环形缓冲区大小（默认: 1024）
        .WithRetention(RetentionPolicy.ByCount.Of(5000))           // 保留最近 N 条消息
        // .WithRetention(RetentionPolicy.ByAge.Of(TimeSpan.FromHours(1)))  // 按存活时间保留
        // .WithRetention(new RetentionPolicy.ByCountOrAge(5000, TimeSpan.FromHours(1)))  // 两者组合
        .WithOverflowPolicy(OverflowPolicy.Block)    // OverwriteOldest | Block | DropNewest
        .WithBackpressure(bp => {
            bp.HighWatermarkPercent = 0.8;            // 80% 时触发高水位告警
            bp.LowWatermarkPercent = 0.3;             // 30% 时触发低水位恢复
            bp.SlowConsumerStrategy = SlowConsumerStrategy.Warn;  // None | Warn | ForceAdvance
            bp.SlowConsumerLagThreshold = 500;        // 消费延迟阈值
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

### 使用方式

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

## 背压监控

```csharp
var monitor = bus.BackpressureMonitor;

// 订阅事件
monitor.HighWatermarkReached += (topic, partition, utilization) =>
    Console.WriteLine($"警告：{topic}/{partition} 利用率达 {utilization:P0}");

monitor.LowWatermarkRecovered += (topic, partition, utilization) =>
    Console.WriteLine($"恢复：{topic}/{partition} 利用率降至 {utilization:P0}");

monitor.SlowConsumerDetected += (topic, partition, groupId, lag) =>
    Console.WriteLine($"慢消费者：组 {groupId} 在 {topic}/{partition} 延迟={lag}");

// 轮询指标
var util = monitor.GetUtilization("orders", 0);       // 缓冲区利用率
var lag = monitor.GetConsumerLag("orders", 0);        // 消费延迟
var blocked = monitor.GetBlockedProducerCount("orders", 0);  // 阻塞的生产者数
```

## 死信队列

```csharp
// IDeadLetterQueue 捕获处理失败的消息
var dlq = new InMemoryDeadLetterQueue(maxErrorsPerTopic: 100);

// 通过 SubscriptionManager 订阅时传入（内部 API）
// 查看错误：
var errors = dlq.GetErrors("orders");
foreach (var (error, message) in errors)
{
    Console.WriteLine($"失败：{error.ErrorType}，Offset {error.Offset}：{error.ErrorMessage}");
}
```

## Topic 统计

```csharp
var topicManager = provider.GetRequiredService<ITopicManager>();
var stats = topicManager.GetTopicStatistics((TopicId)"orders");

Console.WriteLine($"Topic：{stats.Topic}");
Console.WriteLine($"分区数：{stats.PartitionCount}");
Console.WriteLine($"总消息数：{stats.TotalMessages}");
Console.WriteLine($"平均利用率：{stats.AverageUtilization:P1}");

foreach (var (pid, p) in stats.Partitions)
{
    Console.WriteLine($"  分区{pid}：head={p.HeadOffset} tail={p.TailOffset} 消息数={p.MessageCount} 利用率={p.Utilization:P1}");
}
```

## 投递保证

| 模式 | 接口 | 保证 | 适用场景 |
|---|---|---|---|
| 消息总线 | `IMessageBus` | 至少一次 | 任务队列、事件溯源、工作分发 |
| 领域事件总线 | `IDomainEventBus` | 最多一次 | 进程内通知、松耦合 |

## 线程安全

- `AppendOnlyLog` 使用 `Interlocked` 管理 Offset 计数器、`SemaphoreSlim` 处理阻塞写入、`AsyncReaderWriterLock` 保护保留读操作
- `ConsumerGroup` 使用 `ReaderWriterLockSlim` 保护成员/分配变更
- `GroupCoordinator` 使用 `ConcurrentDictionary` 管理组注册表
- `TopicManager` 使用 `ConcurrentDictionary` 管理 Topic 注册表
- `SubscriptionManager` 使用普通 `lock` 追踪任务

## 性能特征

- **零分配分区选择** — FNV-1a 哈希进行 Key 路由（无 SHA256 内存分配）
- **无锁 Offset 计数器** — `Interlocked.Increment` 自增 Head Offset
- **异步通知机制** — 基于 `TaskCompletionSource` 的脉冲信号，无需轮询
- **环形缓冲区** — 固定大小 `StoredEntry?[]`，取模索引，无扩容开销
- **分区扇出消费** — `Task.WhenAny` 监听所有已分配分区，首个响应立即返回

## 运行测试

```bash
dotnet test tests/MemQueue.Tests/
```

## 运行基准测试

```bash
dotnet run --project tests/MemQueue.Benchmarks/
```

## 许可证

本项目遵循仓库中的许可证条款。
