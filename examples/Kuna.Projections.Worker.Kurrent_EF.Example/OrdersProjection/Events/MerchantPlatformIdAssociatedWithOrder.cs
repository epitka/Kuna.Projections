#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

public class MerchantPlatformIdAssociatedWithOrder : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public string MerchantPlatformId { get; set; }
}
