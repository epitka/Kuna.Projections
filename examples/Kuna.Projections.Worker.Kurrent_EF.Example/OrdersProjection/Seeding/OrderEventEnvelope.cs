using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Seeding;

public sealed record OrderEventEnvelope(string StreamName, int SequenceInStream, Event Event)
{
    public string EventTypeName => this.Event.TypeName;
}
