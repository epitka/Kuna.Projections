#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.Events;

public class OrderConfirmed : Event
{
    public OrderConfirmed()
    {
        this.CurrencyCode = "USD";
        this.DownPaymentAmount = 0m;
    }

    [ModelId]
    public Guid OrderId { get; set; }

    public Guid CustomerId { get; set; }

    public Guid MerchantId { get; set; }

    public string MerchantReference { get; set; }

    public decimal Amount { get; set; }

    public string CurrencyCode { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }

    public decimal DownPaymentAmount { get; set; }

    public decimal? TaxAmount { get; set; }

    public decimal? ShippingAmount { get; set; }
}
