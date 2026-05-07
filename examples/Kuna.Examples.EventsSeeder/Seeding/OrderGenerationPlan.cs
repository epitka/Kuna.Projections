namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed class OrderGenerationPlan
{
    private OrderGenerationPlan(
        IReadOnlyList<OrderPlan> orders,
        IReadOnlyList<InterleavedOrderEvent> interleavedEvents)
    {
        this.Orders = orders;
        this.InterleavedEvents = interleavedEvents;
        this.TotalOrders = orders.Count;
        this.TotalEvents = interleavedEvents.Count;
        this.CompleteOrderCount = orders.Count(x => x.IsComplete);
        this.ConfirmedCount = orders.Count(x => x.IsConfirmed);
        this.AbandonedCount = orders.Count(x => x.IsAbandoned);
        this.RefundCount = orders.Count(x => x.HasRefund);
        this.EventTypeCounts = interleavedEvents
                               .GroupBy(x => x.EventTypeName, StringComparer.Ordinal)
                               .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
    }

    public IReadOnlyList<OrderPlan> Orders { get; }

    public IReadOnlyList<InterleavedOrderEvent> InterleavedEvents { get; }

    public int TotalOrders { get; }

    public int TotalEvents { get; }

    public int CompleteOrderCount { get; }

    public int ConfirmedCount { get; }

    public int AbandonedCount { get; }

    public int RefundCount { get; }

    public IReadOnlyDictionary<string, int> EventTypeCounts { get; }

    public static OrderGenerationPlan Create(
        IReadOnlyList<OrderPlan> orders,
        IReadOnlyList<InterleavedOrderEvent> interleavedEvents)
    {
        return new OrderGenerationPlan(orders, interleavedEvents);
    }
}
