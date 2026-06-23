using EventSourcingDb;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Health check that verifies basic connectivity to the configured EventSourcingDB
/// server by pinging it.
/// </summary>
public class EventSourcingDbHealthCheck : IHealthCheck
{
    private readonly IClient client;

    /// <summary>
    /// Initializes the health check with the EventSourcingDB client to probe.
    /// </summary>
    public EventSourcingDbHealthCheck(IClient client)
    {
        this.client = client;
    }

    /// <summary>
    /// Runs the EventSourcingDB connectivity check.
    /// </summary>
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await this.client.PingAsync(cancellationToken);

            return HealthCheckResult.Healthy("EventSourcingDB Connected");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("EventSourcingDB Disconnected", ex);
        }
    }
}
