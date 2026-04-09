namespace Kuna.Projections.Abstractions.Services;

public interface IProjectionStartupTask
{
    Task RunAsync(CancellationToken cancellationToken);
}
