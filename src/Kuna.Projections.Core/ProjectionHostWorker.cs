using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Core;

public sealed class ProjectionHostWorker : BackgroundService
{
    private readonly IHostApplicationLifetime hostApplicationLifetime;
    private readonly ILogger<ProjectionHostWorker> logger;
    private readonly IReadOnlyCollection<IProjectionStartupTask> startupTasks;
    private readonly IReadOnlyCollection<IProjectionPipeline> pipelines;

    public ProjectionHostWorker(
        IHostApplicationLifetime hostApplicationLifetime,
        ILogger<ProjectionHostWorker> logger,
        IEnumerable<IProjectionStartupTask> startupTasks,
        IEnumerable<IProjectionPipeline> pipelines)
    {
        this.hostApplicationLifetime = hostApplicationLifetime;
        this.logger = logger;
        this.startupTasks = startupTasks.ToArray();
        this.pipelines = pipelines.ToArray();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            foreach (var startupTask in this.startupTasks)
            {
                await startupTask.RunAsync(stoppingToken);
            }

            this.logger.LogWarning("Starting projection host worker at {Now}", DateTime.Now);

            await Task.WhenAll(this.pipelines.Select(x => x.RunAsync(stoppingToken)));
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Shutting down projection host worker at {Now} because of unhandled exception",
                DateTime.Now);
        }
        finally
        {
            this.hostApplicationLifetime.StopApplication();
        }

        this.logger.LogWarning("Stopped projection host worker at {Now}", DateTime.Now);
    }
}
