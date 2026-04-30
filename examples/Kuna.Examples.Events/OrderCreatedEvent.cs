#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.Events;

public class OrderCreatedEvent : Event
{
    public OrderCreatedEvent()
    {
        this.CurrencyCode = "USD";
    }

    [ModelId]
    public Guid Id { get; set; }

    public string OrderNumber { get; set; }

    public Guid MerchantId { get; set; }

    public DateTimeOffset CreatedDateTime { get; set; }

    public DateTime? StartDate { get; set; }

    public string MerchantReference { get; set; }

    public string Email { get; set; }

    public string FirstNames { get; set; }

    public string LastName { get; set; }

    public string Phone { get; set; }

    public Address BillingAddress { get; set; }

    public Address PostalAddress { get; set; }

    public decimal? AuthorizationAmount { get; set; }

    public string PaymentAuthorizationId { get; set; }

    public decimal? Amount { get; set; }

    public decimal? ShippingAmount { get; set; }

    public decimal? TaxAmount { get; set; }

    public string CurrencyCode { get; set; }

    public Guid? CustomerId { get; set; }

    public string PaymentSourceId { get; set; }

    public string Source { get; set; }

    public string LocationAuthorizationMethod { get; set; }

    public string LocationId { get; set; }

    public string LocationName { get; set; }

    public string LocationCategories { get; set; }

    public string LocationPhone { get; set; }

    public string LocationAddressLine1 { get; set; }

    public string LocationAddressLine2 { get; set; }

    public string LocationAddressCity { get; set; }

    public string LocationAddressState { get; set; }

    public string LocationAddressPostalCode { get; set; }

    public string LocationAddressCountry { get; set; }

    public double? LocationAddressLatitude { get; set; }

    public double? LocationAddressLongitude { get; set; }

    public bool? DeferMerchantFundsCapture { get; set; }

    public string ProductType { get; set; }
}
