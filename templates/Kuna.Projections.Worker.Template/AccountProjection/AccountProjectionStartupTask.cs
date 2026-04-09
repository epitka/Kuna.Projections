using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Worker.Template.AccountProjection;

public sealed class AccountProjectionStartupTask : IProjectionStartupTask
{
    private readonly IServiceProvider serviceProvider;

    public AccountProjectionStartupTask(IServiceProvider serviceProvider)
    {
        this.serviceProvider = serviceProvider;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var scope = this.serviceProvider.CreateScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<AccountProjectionDbContext>();
        await dbContext.Database.EnsureCreatedAsync(cancellationToken);
    }
}
