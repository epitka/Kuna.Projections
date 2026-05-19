using Microsoft.Extensions.Configuration;

namespace Kuna.Projections.Source.Kafka;

public static class KafkaSourceSettingsResolver
{
    private const string KafkaConnectionStringName = "Kafka";

    public static KafkaSourceSettings Resolve(
        IConfiguration configuration,
        string settingsSectionName)
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(settingsSectionName);

        var sectionPath = $"{settingsSectionName}:{KafkaSourceSettings.SectionName}";
        var sectionSettings = configuration.GetRequiredSection(sectionPath).Get<KafkaSourceSettings>()
                              ?? throw new InvalidOperationException($"Missing configuration section: {sectionPath}");
        var bootstrapServers = configuration.GetConnectionString(KafkaConnectionStringName);

        if (string.IsNullOrWhiteSpace(bootstrapServers))
        {
            throw new InvalidOperationException($"Missing connection string: {KafkaConnectionStringName}");
        }

        var sourceSettings = new KafkaSourceSettings
        {
            BootstrapServers = bootstrapServers,
            Topic = sectionSettings.Topic,
            ClientId = sectionSettings.ClientId,
            ConsumerGroupId = sectionSettings.ConsumerGroupId,
            AutoOffsetReset = sectionSettings.AutoOffsetReset,
            KeyFormat = sectionSettings.KeyFormat,
            Transformer = sectionSettings.Transformer,
            Partitions = sectionSettings.Partitions,
            PollTimeoutMs = sectionSettings.PollTimeoutMs,
        };

        KafkaSourceSettingsValidator.Validate(sourceSettings, sectionPath);
        return sourceSettings;
    }
}
