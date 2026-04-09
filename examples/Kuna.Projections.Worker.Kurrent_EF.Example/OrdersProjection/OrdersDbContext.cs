using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF.Data;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;

public class OrdersDbContext : SqlProjectionsDbContext
{
    public OrdersDbContext(
        DbContextOptions<OrdersDbContext> options,
        ProjectionSchema<OrdersDbContext> projectionSchema)
        : base(options, projectionSchema.Value)
    {
    }

    public DbSet<Order> Orders { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(
            order =>
            {
                order.Property(i => i.MerchantReference).HasMaxLength(100);
                order.HasIndex(i => i.MerchantReference);

                order.HasIndex(i => new { EventNumber = i.EventNumber, });

                order.Property(i => i.MerchantPlatformId).HasMaxLength(100);
                order.HasIndex(i => i.MerchantPlatformId);

                order.Property(i => i.OrderNumber).HasMaxLength(100);
                order.HasIndex(i => i.OrderNumber);

                order.Property(i => i.Source).HasMaxLength(30);
                order.Property(i => i.CurrencyCode).HasMaxLength(3);

                order.HasIndex(i => i.MerchantId);
                order.HasIndex(i => i.CustomerId);

                order.HasIndex(i => i.PaymentAuthorizationId);
                order.Property(i => i.PaymentAuthorizationId).HasMaxLength(100);

                order.HasIndex(i => i.OrderStatus);

                order.OwnsOne(
                    i => i.ShippingAddress,
                    sa =>
                    {
                        sa.Property(sa => sa.City).HasMaxLength(100);
                        sa.Property(sa => sa.Country).HasMaxLength(25);
                        sa.Property(sa => sa.Line1).HasMaxLength(250);
                        sa.Property(sa => sa.Line2).HasMaxLength(250);
                        sa.Property(sa => sa.PostCode).HasMaxLength(50);
                        sa.Property(sa => sa.State).HasMaxLength(100);
                    });

                order.OwnsOne(
                    i => i.BillingAddress,
                    ba =>
                    {
                        ba.Property(ba => ba.City).HasMaxLength(100);
                        ba.Property(ba => ba.Country).HasMaxLength(25);
                        ba.Property(ba => ba.Line1).HasMaxLength(250);
                        ba.Property(ba => ba.Line2).HasMaxLength(250);
                        ba.Property(ba => ba.PostCode).HasMaxLength(50);
                        ba.Property(ba => ba.State).HasMaxLength(100);
                    });

                order.OwnsOne(
                    i => i.Customer,
                    c =>
                    {
                        c.Property(c => c.FirstName).HasMaxLength(150);
                        c.Property(c => c.LastName).HasMaxLength(150);
                        c.Property(c => c.PhoneNumber).HasMaxLength(50);
                        c.Property(c => c.Email).HasMaxLength(250);
                    });

                order.Property(i => i.FeeReferences).HasMaxLength(200);
                order.Property(i => i.CaptureReferences).HasMaxLength(200);
                order.Property(i => i.VoidReferences).HasMaxLength(200);

                order.Property(i => i.Amount).HasMoneyPrecision();
                order.Property(i => i.TaxAmount).HasMoneyPrecision();
                order.Property(i => i.ShippingAmount).HasMoneyPrecision();
                order.Property(i => i.TotalFundsCaptured).HasMoneyPrecision();
                order.Property(i => i.TotalFundsRefunded).HasMoneyPrecision();
                order.Property(i => i.TotalFundsVoided).HasMoneyPrecision();
            });

        modelBuilder.Entity<Refund>(
            model =>
            {
                model.ToTable(nameof(Order.OrderRefunds));

                model.HasKey(refund => refund.Id);

                // Since we are assigning an ID from the event, we need to tell the db to not generate an id.
                model.Property(refund => refund.Id).ValueGeneratedNever();

                model.HasIndex(refund => refund.OrderId);

                model.Property(refund => refund.Amount).HasPrecision(18, 2);
                model.Property(refund => refund.MerchantRefundFeeRebate);
                model.Property(refund => refund.MerchantRefundTransactionFee);
                model.Property(refund => refund.MerchantRefundFeeRebatePercent);
                model.Property(refund => refund.MerchantReference).HasMaxLength(100);
                model.Property(refund => refund.MerchantId);
                model.Property(refund => refund.RefundDateTime);
            });

        base.OnModelCreating(modelBuilder);
    }
}
