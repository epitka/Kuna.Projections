using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Core;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Events;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;
using Kuna.StreamGenerator;
using Model_Address = Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model.Address;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;

public class OrdersProjection : Projection<Order>
{
    private static readonly HashSet<string> IgnoredEvents = new()
    {
        "OrderValidationErrorEvent",
        "MerchantFeeForPaymentPlanAdded",
    };

    internal OrdersProjection(Guid modelId)
        : base(modelId)
    {
    }

    public override void Apply(UnknownEvent @event)
    {
        if (IgnoredEvents.Contains(@event.UnknownEventName))
        {
            return;
        }

        base.Apply(@event);
    }

    public void Apply(RefundAppliedToOrderEvent @event)
    {
        this.ModelState.OrderRefunds.Add(
            new Refund
            {
                Id = @event.RefundId,
                Amount = @event.Amount,
                RefundId = @event.RefundId,
                MerchantReference = @event.MerchantReference,
                OrderId = this.ModelState.Id,
            });

        this.ModelState.TotalFundsRefunded += @event.Amount;
    }

    public void Apply(OrderCreatedEvent @event)
    {
        if (@event.BillingAddress != null)
        {
            this.ModelState.BillingAddress = new Model_Address
            {
                City = @event.BillingAddress.City,
                Country = @event.BillingAddress.Country,
                PostCode = @event.BillingAddress.PostCode,
                Line1 = @event.BillingAddress.Line1,
                Line2 = @event.BillingAddress.Line2,
                State = @event.BillingAddress.State,
            };
        }

        if (@event.PostalAddress != null)
        {
            this.ModelState.ShippingAddress = new Model_Address
            {
                City = @event.PostalAddress.City,
                Country = @event.PostalAddress.Country,
                PostCode = @event.PostalAddress.PostCode,
                Line1 = @event.PostalAddress.Line1,
                Line2 = @event.PostalAddress.Line2,
                State = @event.PostalAddress.State,
            };
        }

        this.ModelState.Customer = new Customer
        {
            FirstName = @event.FirstNames,
            LastName = @event.LastName,
            PhoneNumber = @event.Phone,
            Email = @event.Email,
        };

        //// this.ModelState.Id = @event.Id;
        this.ModelState.CreatedDateTime = @event.CreatedDateTime;
        this.ModelState.OrderStatus = OrderStatus.Created;
        this.ModelState.OrderNumber = @event.OrderNumber;
        this.ModelState.MerchantReference = @event.MerchantReference;
        this.ModelState.Amount = @event.Amount!.Value;
        this.ModelState.ShippingAmount = @event.ShippingAmount;
        this.ModelState.TaxAmount = @event.TaxAmount;
        this.ModelState.CurrencyCode = @event.CurrencyCode;
        this.ModelState.CustomerId = @event.CustomerId;
        this.ModelState.Source = @event.Source;
        this.ModelState.PaymentAuthorizationId = @event.PaymentAuthorizationId;
        this.ModelState.MerchantId = @event.MerchantId;
    }

    public void Apply(OrderConfirmedEvent @event)
    {
        this.ModelState.CompletedDateTime = @event.CompletedDateTime;
        this.ModelState.OrderStatus = OrderStatus.Confirmed;
        this.ModelState.TaxAmount = @event.TaxAmount;
        this.ModelState.Amount = @event.Amount;
        this.ModelState.ShippingAmount = @event.ShippingAmount;
    }

    public void Apply(OrderAbandondedEvent @event)
    {
        this.ModelState.OrderStatus = OrderStatus.Abandoned;
    }

    public void Apply(OrderDeclinedEvent @event)
    {
        this.ModelState.OrderStatus = OrderStatus.Declined;
    }

    public void Apply(MerchantPlatformIdAssociatedWithOrder @event)
    {
        this.ModelState.MerchantPlatformId = @event.MerchantPlatformId;
    }

    public void Apply(OrderAmountVoidedEvent @event)
    {
        if (this.ModelState.VoidReferences != null
            && this.ModelState.VoidReferences.Contains(@event.MerchantReference))
        {
            return;
        }

        this.ModelState.VoidReferences += @event.MerchantReference + '|';
        this.ModelState.TotalFundsVoided += @event.Amount;
    }

    public void Apply(MerchantOrderRefundProcessed @event)
    {
        this.ModelState.OrderRefunds.Add(
            new Refund
            {
                Id = @event.RefundId,
                Amount = @event.Amount,
                RefundId = @event.RefundId,
                MerchantReference = @event.MerchantRefundReference,
                OrderId = this.ModelState.Id,
                MerchantId = @event.MerchantId,
                MerchantRefundFeeRebate = @event.MerchantRefundFeeRebate,
                MerchantRefundTransactionFee = @event.MerchantRefundTransactionFee,
                MerchantRefundFeeRebatePercent = @event.MerchantRefundFeeRebatePercent,
                RefundDateTime = @event.RefundedDateTime,
            });

        this.ModelState.TotalFundsRefunded += @event.Amount;
    }

    public void Apply(FundsCapturedOnOrderEvent @event)
    {
        if (this.ModelState.CaptureReferences != null
            && this.ModelState.CaptureReferences.Contains(@event.MerchantReference))
        {
            return;
        }

        this.ModelState.CaptureReferences += @event.MerchantReference + '|';
        this.ModelState.TotalFundsCaptured += @event.Amount;
    }

    public void Apply(OrderMerchantFeesCalculatedEvent @event)
    {
        // idempotency check - this event will always fire first and set some value initially on this value,
        // so if it is not null, this event has already happened.
        if (this.ModelState.MerchantTransactionFeeAmount != null)
        {
            return;
        }

        this.ModelState.MerchantTransactionFeeAmount = @event.MerchantTransactionFeeAmount;
        this.ModelState.MerchantTransactionFeePercent = @event.MerchantTransactionFeePercent;
        this.ModelState.MerchantTransactionFeePercentCalculated = @event.MerchantTransactionFeePercentCalculated;
    }

    public void Apply(MerchantTransactionFeeAddedToOrderEvent @event)
    {
        if (this.ModelState.FeeReferences != null
            && this.ModelState.FeeReferences.Contains(@event.MerchantReference))
        {
            return;
        }

        this.ModelState.FeeReferences += @event.MerchantReference + '|';
        this.ModelState.MerchantTransactionFeeAmount += @event.MerchantTransactionFeeAmount;
    }

    public void Apply(OrderCustomerLinkedEvent @event)
    {
        this.ModelState.CustomerId = @event.CustomerId;
    }

    public void Apply(OrderAmountIncreased @event)
    {
        this.ModelState.Amount = @event.NewOrderAmount;
    }
}
