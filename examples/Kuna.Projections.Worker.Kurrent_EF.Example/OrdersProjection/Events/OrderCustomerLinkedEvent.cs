#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

public class OrderCustomerLinkedEvent : Event
{
    public Guid CustomerId { get; set; }

    public string IpAddress { get; set; }

    [ModelId]
    public Guid OrderId { get; set; }
}
