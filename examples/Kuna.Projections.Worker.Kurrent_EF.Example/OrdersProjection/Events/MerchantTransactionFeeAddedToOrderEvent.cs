#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

public class MerchantTransactionFeeAddedToOrderEvent : Event
{
    [ModelId]
    public Guid OrderId { get; set; }

    public decimal MerchantTransactionFeeAmount { get; set; }

    public string MerchantReference { get; set; }

    public string CurrencyCode { get; set; }

    public DateTime TransactionDate { get; set; }

    public Guid MerchantId { get; set; }

    public string ProductType { get; set; }
}
