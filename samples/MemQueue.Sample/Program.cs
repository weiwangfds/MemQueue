using MemQueue.Abstractions;
using MemQueue.DependencyInjection;
using MemQueue.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MemQueue.Sample;

public record OrderCreated(Guid OrderId, string Product, int Quantity) : MessageBase;
public record OrderShipped(Guid OrderId, string TrackingNumber) : MessageBase;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        services.AddMemQueue(builder => builder
            .AddTopic("orders", topic => topic
                .WithPartitions(4)
                .WithRetention(RetentionPolicy.ByCount.Of(10_000)))
            .AddTopic("shipping", topic => topic
                .WithPartitions(2)));

        var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<IMessageBus>();

        using var orderCts = new CancellationTokenSource();
        using var shippingCts = new CancellationTokenSource();

        await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", async (msg, ctx, ct) =>
        {
            Console.WriteLine($"[Broadcast] Order {msg.OrderId}: {msg.Quantity}x {msg.Product} (partition={ctx.Partition}, offset={ctx.Offset})");
            await ValueTask.CompletedTask;
        }, orderCts.Token);

        await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", (ConsumerGroupId)"order-processors", async (msg, ctx, ct) =>
        {
            Console.WriteLine($"[Group:processor-1] Processing order {msg.OrderId}: {msg.Product}");
            await ctx.CommitAsync(ct);
        }, orderCts.Token);

        await bus.SubscribeAsync<OrderCreated>((TopicId)"orders", (ConsumerGroupId)"order-processors", async (msg, ctx, ct) =>
        {
            Console.WriteLine($"[Group:processor-2] Processing order {msg.OrderId}: {msg.Product}");
            await ctx.CommitAsync(ct);
        }, orderCts.Token);

        await bus.SubscribeAsync<OrderShipped>((TopicId)"shipping", async (msg, ctx, ct) =>
        {
            Console.WriteLine($"[Shipping] Order {msg.OrderId} shipped: {msg.TrackingNumber}");
            await ValueTask.CompletedTask;
        }, shippingCts.Token);

        var producer = bus.CreateProducer<OrderCreated>((TopicId)"orders");
        var shippingProducer = bus.CreateProducer<OrderShipped>((TopicId)"shipping");

        for (var i = 0; i < 5; i++)
        {
            var orderId = Guid.NewGuid();
            await producer.ProduceAsync(new OrderCreated(orderId, $"Product-{i}", i + 1));
            Console.WriteLine($"Produced order {orderId}");
        }

        await Task.Delay(500);

        for (var i = 0; i < 3; i++)
        {
            var tracking = $"TRK-{Random.Shared.Next(100000, 999999)}";
            await shippingProducer.ProduceAsync(new OrderShipped(Guid.NewGuid(), tracking));
        }

        await Task.Delay(500);

        Console.WriteLine("\n--- Message Replay Demo ---");
        var replayConsumer = bus.CreateConsumer<OrderCreated>((TopicId)"orders");
        await replayConsumer.SeekToBeginningAsync(new PartitionId(0));
        var replayMsg = await replayConsumer.ConsumeAsync();
        Console.WriteLine($"[Replay] First message: {replayMsg.Value.Product}");

        orderCts.Cancel();
        shippingCts.Cancel();

        await bus.DisposeAsync();

        Console.WriteLine("\nDone. Press any key to exit.");
    }
}
