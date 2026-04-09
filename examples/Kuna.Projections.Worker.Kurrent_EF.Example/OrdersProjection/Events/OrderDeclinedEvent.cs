#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

public class OrderDeclinedEvent : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }
}
