using KurrentDB.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Health check that verifies basic connectivity to the configured Kurrent
/// server by attempting to read a single event from the global stream.
/// </summary>
public class KurrentDbHealthCheck : IHealthCheck
{
    private readonly KurrentDBClient eventStoreClient;

    /// <summary>
    /// Initializes the health check with the Kurrent client to probe.
    /// </summary>
    public KurrentDbHealthCheck(KurrentDBClient eventStoreClient)
    {
        this.eventStoreClient = eventStoreClient;
    }

    /// <summary>
    /// Runs the Kurrent connectivity check.
    /// </summary>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        return this.CheckKurrentDbConnectionAsync(cancellationToken);
    }

    private async Task<HealthCheckResult> CheckKurrentDbConnectionAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var ignored in this.eventStoreClient.ReadAllAsync(
                               Direction.Backwards,
                               Position.End,
                               maxCount: 1,
                               resolveLinkTos: false,
                               cancellationToken: cancellationToken))
            {
                break;
            }

            return HealthCheckResult.Healthy("KurrentDB Connected");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("KurrentDB Disconnected", ex);
        }
    }
}
