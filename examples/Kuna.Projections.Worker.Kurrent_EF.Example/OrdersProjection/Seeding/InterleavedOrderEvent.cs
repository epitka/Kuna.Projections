using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Seeding;

public sealed record InterleavedOrderEvent(string StreamName, Event Event, int SequenceInStream)
{
    public string EventTypeName => this.Event.TypeName;
}
