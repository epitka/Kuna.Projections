#nullable disable

using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.Events;

public class OrderCreated : Event
{
    public OrderCreated()
    {
        this.CurrencyCode = "USD";
    }

    [ModelId]
    public Guid Id { get; set; }

    public string OrderNumber { get; set; }

    public Guid MerchantId { get; set; }

    public DateTimeOffset CreatedDateTime { get; set; }

    public string MerchantReference { get; set; }

    public string Email { get; set; }

    public string FirstNames { get; set; }

    public string LastName { get; set; }

    public string Phone { get; set; }

    public Address BillingAddress { get; set; }

    public Address PostalAddress { get; set; }

    public string PaymentAuthorizationId { get; set; }

    public decimal? Amount { get; set; }

    public decimal? ShippingAmount { get; set; }

    public decimal? TaxAmount { get; set; }

    public string CurrencyCode { get; set; }

    public Guid? CustomerId { get; set; }

    public string Source { get; set; }

    public string ProductType { get; set; }
}
