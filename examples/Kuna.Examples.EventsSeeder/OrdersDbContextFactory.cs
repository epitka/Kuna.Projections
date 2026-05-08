using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;

public sealed class OrdersDbContextFactory : IDesignTimeDbContextFactory<OrdersDbContext>
{
    public OrdersDbContext CreateDbContext(string[] args)
    {
        const string connectionString = "Host=localhost;Port=5432;Database=orders_projection;Username=postgres;Password=postgres";
        var optionsBuilder = new DbContextOptionsBuilder<OrdersDbContext>();

        optionsBuilder.UseNpgsql(connectionString);

        return new OrdersDbContext(optionsBuilder.Options, new ProjectionSchema<OrdersDbContext>("dbo"));
    }
}
