using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Pipeline.Integration.Test.LocalOrders;

public sealed class OrdersDbContext : SqlProjectionsDbContext
{
    private const string ProjectionSchema = "dbo";

    public OrdersDbContext(DbContextOptions options)
        : base(options, ProjectionSchema)
    {
    }

    public DbSet<Order> Orders { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(
            order =>
            {
                order.HasIndex(i => i.OrderNumber);
                order.HasIndex(i => i.OrderStatus);
                order.HasIndex(i => i.MerchantId);
                order.HasIndex(i => i.CustomerId);
                order.HasIndex(i => i.PaymentAuthorizationId);

                order.Property(i => i.OrderNumber).HasMaxLength(100);
                order.Property(i => i.PaymentAuthorizationId).HasMaxLength(100);
                order.Property(i => i.CurrencyCode).HasMaxLength(3);
                order.Property(i => i.Source).HasMaxLength(30);
                order.Property(i => i.MerchantPlatformId).HasMaxLength(100);
                order.Property(i => i.MerchantReference).HasMaxLength(100);
                order.Property(i => i.FeeReferences).HasMaxLength(200);
                order.Property(i => i.CaptureReferences).HasMaxLength(200);
                order.Property(i => i.VoidReferences).HasMaxLength(200);
                order.Property(i => i.CreatedDateTime).HasPrecision(6);
                order.Property(i => i.CompletedDateTime).HasPrecision(6);

                order.Property(i => i.Amount).HasColumnType("decimal(18,2)");
                order.Property(i => i.TaxAmount).HasColumnType("decimal(18,2)");
                order.Property(i => i.ShippingAmount).HasColumnType("decimal(18,2)");
                order.Property(i => i.MerchantTransactionFeeAmount).HasColumnType("decimal(18,2)");
                order.Property(i => i.TotalFundsCaptured).HasColumnType("decimal(18,2)");
                order.Property(i => i.TotalFundsVoided).HasColumnType("decimal(18,2)");
                order.Property(i => i.TotalFundsRefunded).HasColumnType("decimal(18,2)");
            });

        modelBuilder.Entity<Refund>(
            refund =>
            {
                refund.ToTable(nameof(Order.OrderRefunds));
                refund.HasKey(x => x.Id);
                refund.Property(x => x.Id).ValueGeneratedNever();
                refund.HasIndex(x => x.OrderId);
                refund.Property(x => x.Amount).HasColumnType("decimal(18,2)");
                refund.Property(x => x.MerchantReference).HasMaxLength(100);
                refund.Property(x => x.MerchantRefundFeeRebate).HasColumnType("decimal(18,2)");
                refund.Property(x => x.MerchantRefundTransactionFee).HasColumnType("decimal(18,2)");
                refund.Property(x => x.MerchantRefundFeeRebatePercent).HasColumnType("decimal(18,2)");
                refund.Property(x => x.RefundDateTime).HasPrecision(6);
            });

        base.OnModelCreating(modelBuilder);
    }
}
