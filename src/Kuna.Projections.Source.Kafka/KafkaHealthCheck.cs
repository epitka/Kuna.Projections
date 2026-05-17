using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaHealthCheck : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(HealthCheckResult.Healthy("Kafka health check is not implemented yet."));
    }
}
