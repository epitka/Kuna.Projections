using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;

public sealed class OrdersProjectionStartupTask : IProjectionStartupTask
{
    private readonly IServiceProvider serviceProvider;

    public OrdersProjectionStartupTask(IServiceProvider serviceProvider)
    {
        this.serviceProvider = serviceProvider;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var scope = this.serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
        await dbContext.Database.EnsureCreatedAsync(cancellationToken);
    }
}
