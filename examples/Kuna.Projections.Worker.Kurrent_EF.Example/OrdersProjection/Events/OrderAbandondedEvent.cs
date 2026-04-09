#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.StreamGenerator;

public class OrderAbandondedEvent : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }
}
