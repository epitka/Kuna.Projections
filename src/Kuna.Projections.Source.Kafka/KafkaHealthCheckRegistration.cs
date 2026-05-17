namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaHealthCheckRegistration
{
    public required string SettingsSectionName { get; init; }

    public required KafkaSourceSettings SourceSettings { get; init; }
}
