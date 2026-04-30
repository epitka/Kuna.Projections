#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.EventsSeeder.Events;

public class OrderDeclined : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }
}
