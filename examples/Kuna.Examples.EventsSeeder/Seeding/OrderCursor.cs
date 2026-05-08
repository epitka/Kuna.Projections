namespace Kuna.Examples.EventsSeeder.Seeding;

internal sealed class OrderCursor
{
    private int index;

    public OrderCursor(OrderPlan order)
    {
        this.Order = order;
    }

    public OrderPlan Order { get; }

    public bool HasNext => this.index < this.Order.Events.Count;

    public OrderEventEnvelope Next()
    {
        if (!this.HasNext)
        {
            throw new InvalidOperationException("No more events in cursor.");
        }

        return this.Order.Events[this.index++];
    }
}
