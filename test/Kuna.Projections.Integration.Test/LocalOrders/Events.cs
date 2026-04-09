using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Pipeline.Integration.Test.LocalOrders;

public sealed class OrderCreatedEvent : Event
{
    public Guid Id { get; set; }

    public string? OrderNumber { get; set; }

    public Guid MerchantId { get; set; }

    public DateTimeOffset CreatedDateTime { get; set; }

    public string? PaymentAuthorizationId { get; set; }

    public decimal? Amount { get; set; }

    public decimal? ShippingAmount { get; set; }

    public decimal? TaxAmount { get; set; }

    public string? CurrencyCode { get; set; }

    public Guid? CustomerId { get; set; }

    public string? ProductType { get; set; }
}

public sealed class OrderMerchantFeesCalculatedEvent : Event
{
    public Guid OrderId { get; set; }

    public decimal MerchantTransactionFeeAmount { get; set; }
}

public sealed class OrderConfirmedEvent : Event
{
    public Guid OrderId { get; set; }

    public string? CurrencyCode { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }
}

public sealed class OrderAbandondedEvent : Event
{
    public Guid OrderId { get; set; }

    public DateTimeOffset? CompletedDateTime { get; set; }
}

public sealed class RefundAppliedToOrderEvent : Event
{
    public Guid OrderId { get; set; }

    public decimal Amount { get; set; }

    public Guid RefundId { get; set; }

    public string? MerchantReference { get; set; }
}
