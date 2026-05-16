using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record InterleavedOrderEvent(string StreamName, Event Event, int SequenceInStream)
{
    public string EventTypeName => this.Event.TypeName;
}
