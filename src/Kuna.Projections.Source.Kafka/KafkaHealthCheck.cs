using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaHealthCheck : IHealthCheck
{
    private readonly IReadOnlyCollection<KafkaHealthCheckRegistration> registrations;
    private readonly IKafkaConsumerFactory consumerFactory;

    public KafkaHealthCheck(
        IEnumerable<KafkaHealthCheckRegistration> registrations,
        IKafkaConsumerFactory consumerFactory)
    {
        this.registrations = registrations.ToArray();
        this.consumerFactory = consumerFactory;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (this.registrations.Count == 0)
        {
            return Task.FromResult(HealthCheckResult.Healthy("No Kafka projections are registered."));
        }

        try
        {
            foreach (var registration in this.registrations)
            {
                using var consumer = this.consumerFactory.Create(
                    registration.SourceSettings,
                    KafkaConsumerGroupIdResolver.ResolveHealthCheck(
                        registration.SourceSettings,
                        registration.SettingsSectionName));

                var partitions = consumer.GetPartitions(registration.SourceSettings.Topic);

                if (partitions.Count == 0)
                {
                    return Task.FromResult(
                        HealthCheckResult.Unhealthy(
                            $"Kafka topic '{registration.SourceSettings.Topic}' configured in '{registration.SettingsSectionName}' has no partitions."));
                }

                if (registration.SourceSettings.Partitions is not { Length: > 0, })
                {
                    continue;
                }

                var missingPartitions = registration.SourceSettings.Partitions
                                                    .Except(partitions)
                                                    .OrderBy(x => x)
                                                    .ToArray();

                if (missingPartitions.Length > 0)
                {
                    return Task.FromResult(
                        HealthCheckResult.Unhealthy(
                            $"Kafka topic '{registration.SourceSettings.Topic}' configured in '{registration.SettingsSectionName}' does not contain partitions: {string.Join(", ", missingPartitions)}."));
                }
            }

            return Task.FromResult(HealthCheckResult.Healthy($"Kafka reachable for {this.registrations.Count} projection source(s)."));
        }
        catch (Exception ex)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka disconnected", ex));
        }
    }
}
