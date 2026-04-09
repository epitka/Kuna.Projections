#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.StreamGenerator;

public class RefundAppliedToOrderEvent : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public decimal Amount { get; set; }

    public Guid RefundId { get; set; }

    public string MerchantReference { get; set; }
}
