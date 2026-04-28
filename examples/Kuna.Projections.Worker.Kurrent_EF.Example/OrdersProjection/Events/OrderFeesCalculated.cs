#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

public class OrderFeesCalculated : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public Guid MerchantId { get; set; }

    public decimal OrderAmount { get; set; }

    public decimal FeeAmount { get; set; }
}
