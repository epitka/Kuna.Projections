namespace Kuna.Projections.Source.Kafka;

internal static class KafkaSourceSettingsValidator
{
    public static void Validate(
        KafkaSourceSettings settings,
        string sectionPath)
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionPath);

        if (string.IsNullOrWhiteSpace(settings.BootstrapServers))
        {
            throw new InvalidOperationException($"Missing required configuration value: {sectionPath}:BootstrapServers");
        }

        if (string.IsNullOrWhiteSpace(settings.Topic))
        {
            throw new InvalidOperationException($"Missing required configuration value: {sectionPath}:Topic");
        }

        if (settings.PollTimeoutMs <= 0)
        {
            throw new InvalidOperationException($"{sectionPath}:PollTimeoutMs must be greater than zero.");
        }

        if (settings.Partitions is null)
        {
            return;
        }

        var duplicatePartitions = settings.Partitions
                                          .GroupBy(x => x)
                                          .Where(x => x.Count() > 1)
                                          .Select(x => x.Key)
                                          .ToArray();

        if (duplicatePartitions.Length > 0)
        {
            throw new InvalidOperationException($"{sectionPath}:Partitions contains duplicate partition ids: {string.Join(", ", duplicatePartitions)}.");
        }

        if (settings.Partitions.Any(partition => partition < 0))
        {
            throw new InvalidOperationException($"{sectionPath}:Partitions must contain only non-negative partition ids.");
        }
    }
}
