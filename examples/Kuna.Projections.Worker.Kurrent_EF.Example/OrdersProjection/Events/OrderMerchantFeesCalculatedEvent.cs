#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.StreamGenerator;

public class OrderMerchantFeesCalculatedEvent : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public Guid MerchantId { get; set; }

    public decimal OrderAmount { get; set; }

    public decimal MerchantTransactionFeeAmount { get; set; }

    public decimal MerchantTransactionFeePercent { get; set; }

    public decimal MerchantTransactionFeePercentCalculated { get; set; }
}
