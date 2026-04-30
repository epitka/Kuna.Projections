#nullable disable

namespace Kuna.Examples.Projections.Orders.Model;

public sealed class Order
    : Kuna.Projections.Abstractions.Models.Model
{
    public decimal Amount { get; set; }

    public decimal? TaxAmount { get; set; }

    public decimal? ShippingAmount { get; set; }

    public decimal? MerchantTransactionFeeAmount { get; set; }

    public decimal? MerchantTransactionFeePercent { get; set; }

    public decimal MerchantTransactionFeePercentCalculated { get; set; }

    public string OrderNumber { get; set; }

    public Address ShippingAddress { get; set; }

    public Address BillingAddress { get; set; }

    public Customer Customer { get; set; }

    public OrderStatus OrderStatus { get; set; }

    public DateTimeOffset CreatedDateTime { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }

    public Guid? CustomerId { get; set; }

    public Guid MerchantId { get; set; }

    public decimal TotalFundsCaptured { get; set; }

    public decimal TotalFundsVoided { get; set; }

    public decimal TotalFundsRefunded { get; set; }

    public string Source { get; set; }

    public string MerchantPlatformId { get; set; }

    public string CurrencyCode { get; set; }

    public string MerchantReference { get; set; }

    public string PaymentAuthorizationId { get; set; }

    public string CaptureReferences { get; set; }

    public string FeeReferences { get; set; }

    public string VoidReferences { get; set; }

    public IList<Refund> OrderRefunds { get; set; } = new List<Refund>();
}
