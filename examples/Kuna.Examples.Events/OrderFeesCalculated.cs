#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.Events;

public class OrderFeesCalculated : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public Guid MerchantId { get; set; }

    public decimal OrderAmount { get; set; }

    public decimal FeeAmount { get; set; }
}
