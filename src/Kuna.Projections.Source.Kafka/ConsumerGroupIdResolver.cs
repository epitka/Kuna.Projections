using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.Kafka;

public static class ConsumerGroupIdResolver
{
    public static string ResolveProjection<TState>(
        KafkaSourceSettings sourceSettings,
        IProjectionSettings<TState> projectionSettings)
        where TState : class, IModel, new()
    {
        ArgumentNullException.ThrowIfNull(projectionSettings);

        return ResolveProjection(
            sourceSettings,
            ProjectionModelName.For<TState>(),
            projectionSettings.InstanceId);
    }

    public static string ResolveStatus(
        KafkaSourceSettings sourceSettings,
        string modelName,
        string instanceId)
    {
        return $"{ResolveProjection(sourceSettings, modelName, instanceId)}-status";
    }

    public static string ResolveReplay(
        KafkaSourceSettings sourceSettings,
        string modelName,
        string instanceId,
        string replayId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(replayId);
        return $"{ResolveProjection(sourceSettings, modelName, instanceId)}-replay-{Normalize(replayId)}";
    }

    public static string ResolveHealthCheck(
        KafkaSourceSettings sourceSettings,
        string settingsSectionName)
    {
        ArgumentNullException.ThrowIfNull(sourceSettings);
        ArgumentException.ThrowIfNullOrWhiteSpace(settingsSectionName);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceSettings.ConsumerGroupId);

        return $"{sourceSettings.ConsumerGroupId}-healthcheck";
    }

    private static string ResolveProjection(
        KafkaSourceSettings sourceSettings,
        string modelName,
        string instanceId)
    {
        ArgumentNullException.ThrowIfNull(sourceSettings);
        ArgumentException.ThrowIfNullOrWhiteSpace(modelName);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentException.ThrowIfNullOrWhiteSpace(sourceSettings.ConsumerGroupId);

        return sourceSettings.ConsumerGroupId;
    }

    private static string Normalize(string value)
    {
        return value.Replace(":", "-", StringComparison.Ordinal)
                    .Replace(".", "-", StringComparison.Ordinal)
                    .Replace(" ", "-", StringComparison.Ordinal);
    }
}
