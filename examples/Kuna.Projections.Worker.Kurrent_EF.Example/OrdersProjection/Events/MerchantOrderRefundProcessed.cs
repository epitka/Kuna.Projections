#nullable disable
using System.ComponentModel;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;

[Description("This event indicates the merchant has processed a refund for this order id")]
public class MerchantOrderRefundProcessed : Event
{
    [Description("The Id of the order being refunded")]
    [ModelId]
    public Guid OrderId { get; set; }

    [Description("The order number of the order being refund")]
    public string OrderNumber { get; set; }

    [Description("The identifier of the refund initiated")]
    public Guid RefundId { get; set; }

    [Description("The identifier of the merchant initiating the refund")]
    public Guid MerchantId { get; set; }

    [Description("The identifier of the customer whose order is being refunded")]
    public Guid CustomerId { get; set; }

    [Description("Indicates the datetime at which this refund was initiated")]
    public DateTimeOffset RefundedDateTime { get; set; }

    [Description("The product type of the order being refunded eg. virtual, classic")]
    public string ProductType { get; set; }

    [Description("The merchant-provided identifier of this refund")]
    public string MerchantRefundReference { get; set; }

    [Description("The amount being refunded")]
    public decimal Amount { get; set; }

    [Description("The fee rebate to be generated as a result of this refund")]
    public decimal MerchantRefundFeeRebate { get; set; }

    [Description("The calculated fee rebate percent to be generated as a result of this refund")]
    public decimal MerchantRefundFeeRebatePercent { get; set; }

    [Description("The transaction fee generated for the merchant to processing this refund")]
    public decimal MerchantRefundTransactionFee { get; set; }

    [Description("The indicator of the source of the refund")]
    public string ClientId { get; set; }

    [Description("The code of the currency this refund was issued in")]
    public string CurrencyCode { get; set; }

    [Description("Indicates whether or not this refund has been triggered by a void")]
    public bool IsTriggeredByVoid { get; set; }
}
