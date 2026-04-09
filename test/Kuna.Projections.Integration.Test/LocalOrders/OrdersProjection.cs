using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Core;

namespace Kuna.Projections.Pipeline.Integration.Test.LocalOrders;

public sealed class OrdersProjection : Projection<Order>
{
    internal OrdersProjection(Guid modelId)
        : base(modelId)
    {
    }

    public override void Apply(UnknownEvent @event)
    {
        base.Apply(@event);
    }

    public void Apply(OrderCreatedEvent @event)
    {
        //// this.ModelState.Id = @event.Id;
        this.ModelState.CreatedDateTime = @event.CreatedDateTime;
        this.ModelState.OrderStatus = OrderStatus.Created;
        this.ModelState.OrderNumber = @event.OrderNumber;
        this.ModelState.Amount = @event.Amount.GetValueOrDefault();
        this.ModelState.ShippingAmount = @event.ShippingAmount;
        this.ModelState.TaxAmount = @event.TaxAmount;
        this.ModelState.CurrencyCode = @event.CurrencyCode;
        this.ModelState.CustomerId = @event.CustomerId;
        this.ModelState.PaymentAuthorizationId = @event.PaymentAuthorizationId;
        this.ModelState.MerchantId = @event.MerchantId;
    }

    public void Apply(OrderMerchantFeesCalculatedEvent @event)
    {
        this.ModelState.MerchantTransactionFeeAmount = @event.MerchantTransactionFeeAmount;
    }

    public void Apply(OrderConfirmedEvent @event)
    {
        this.ModelState.CompletedDateTime = @event.CompletedDateTime;
        this.ModelState.OrderStatus = OrderStatus.Confirmed;
    }

    public void Apply(OrderAbandondedEvent @event)
    {
        this.ModelState.OrderStatus = OrderStatus.Abandoned;
    }

    public void Apply(RefundAppliedToOrderEvent @event)
    {
        this.ModelState.OrderRefunds.Add(
            new Refund
            {
                Id = @event.RefundId,
                OrderId = this.ModelState.Id,
                RefundId = @event.RefundId,
                Amount = @event.Amount,
                MerchantReference = @event.MerchantReference,
            });

        this.ModelState.TotalFundsRefunded += @event.Amount;
    }
}
