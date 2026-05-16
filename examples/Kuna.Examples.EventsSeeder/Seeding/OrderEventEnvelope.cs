using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record OrderEventEnvelope(string StreamName, int SequenceInStream, Event Event)
{
    public string EventTypeName => this.Event.TypeName;
}
