#nullable disable
using System.ComponentModel;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

[Description("This indicates an order amount has been increased by a merchant")]
public class OrderAmountIncreased : Event
{
    [Description("The id of the order")]
    [ModelId]
    public Guid OrderId { get; set; }

    [Description("The id of the merchant")]
    public Guid MerchantId { get; set; }

    [Description("The new order amount")]
    public decimal NewOrderAmount { get; set; }

    [Description("Added amount")]
    public decimal AddedAmount { get; set; }

    [Description("The currency code representing the currency used for the void")]
    public string CurrencyCode { get; set; }
}
