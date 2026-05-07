using Kuna.StreamGenerator;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Seeding;

public static class OrderStreamWritePlanFactory
{
    public static StreamWritePlan Create(OrderGenerationPlan plan)
    {
        return new StreamWritePlan(
            plan.InterleavedEvents
                .Select(x => new StreamWriteEvent(x.StreamName, x.EventTypeName, x.Event))
                .ToArray());
    }
}
