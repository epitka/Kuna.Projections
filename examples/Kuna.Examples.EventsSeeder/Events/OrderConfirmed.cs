#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.EventsSeeder.Events;

public class OrderConfirmedEvent : Event
{
    public OrderConfirmedEvent()
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

    public DateTimeOffset OrderCreatedDateTime { get; set; }

    public decimal? EstablishmentFee { get; set; }

    public decimal? InstallmentServiceFee { get; set; }

    public decimal FirstInstallmentPercentage { get; set; }

    public decimal? InterestRate { get; set; }

    public int NumberOfInstallments { get; set; }

    public bool? DeferMerchantFundsCapture { get; set; }

    public decimal FirstInstallmentPaymentAuthorizationAmount { get; set; }

    public string FirstInstallmentPaymentAuthorizationId { get; set; }

    public string FirstInstallmentCustomerPaymentSourceId { get; set; }

    public decimal DownPaymentAmount { get; set; }

    public decimal? TaxAmount { get; set; }

    public decimal? ShippingAmount { get; set; }
}
