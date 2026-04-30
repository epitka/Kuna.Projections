using Kuna.Projections.Abstractions.Models;

namespace Kuna.Examples.Projections.OrdersProjection.Model;

public class Refund : ChildEntity
{
    public Guid Id { get; set; }

    public Guid OrderId { get; set; }

    public decimal Amount { get; set; }

    public Guid RefundId { get; set; }

    public Guid MerchantId { get; set; }

    public string? MerchantReference { get; set; }

    public decimal MerchantRefundFeeRebate { get; set; }

    public decimal MerchantRefundFeeRebatePercent { get; set; }

    public decimal MerchantRefundTransactionFee { get; set; }

    public DateTimeOffset? RefundDateTime { get; set; }
}
