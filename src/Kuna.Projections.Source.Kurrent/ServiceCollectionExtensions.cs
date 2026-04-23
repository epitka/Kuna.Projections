using System.Reflection;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using KurrentDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Registers the Kurrent-backed event source, health checks, and supporting
/// services required to feed projection envelopes into the core pipeline.
/// </summary>
public static class ServiceCollectionExtensions
{
    private const string KurrentDbConnectionStringName = "KurrentDB";

    /// <summary>
    /// Adds the Kurrent source implementation and related configuration for the
    /// specified projection model state type.
    /// </summary>
    public static IServiceCollection AddKurrentDBSource<TState>(
        this IServiceCollection services,
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        string? settingsSectionName = null)
        where TState : class, IModel, new()
    {
        var assembly = Assembly.GetEntryAssembly();
        var exportedTypes = assembly!.GetExportedTypes();
        var resolvedSettingsSectionName = string.IsNullOrWhiteSpace(settingsSectionName)
                                              ? ProjectionSettingsSection.Name
                                              : settingsSectionName;

        services.AddSingleton<IEventDeserializer>(
            provider =>
            {
                var eventTypes = exportedTypes
                                 .Where(x => x.IsSubclassOf(typeof(Event)))
                                 .ToArray();

                return new EventDeserializer(eventTypes, loggerFactory.CreateLogger<EventDeserializer>());
            });

        var projectionSettings = configuration
                                 .GetRequiredSection(resolvedSettingsSectionName)
                                 .Get<ProjectionSettings<TState>>()
                                 ?? throw new InvalidOperationException($"Missing configuration section: {resolvedSettingsSectionName}");

        services.TryAddSingleton<IProjectionSettings<TState>>(projectionSettings);

        services.AddSingleton(
            provider =>
            {
                var resolvedProjectionSettings = provider.GetRequiredService<IProjectionSettings<TState>>();

                if (resolvedProjectionSettings.Source != ProjectionSourceKind.KurrentDB)
                {
                    throw new InvalidOperationException(
                        $"Unsupported projection source '{resolvedProjectionSettings.Source}' for section '{resolvedSettingsSectionName}'.");
                }

                var kurrentSectionPath = $"{resolvedSettingsSectionName}:{KurrentDbSourceSettings.SectionName}";
                var kurrentSection = configuration.GetSection(kurrentSectionPath);

                if (!kurrentSection.Exists())
                {
                    throw new InvalidOperationException($"Missing required configuration section: {kurrentSectionPath}");
                }

                var sourceSettings = kurrentSection.Get<KurrentDbSourceSettings>()
                                     ?? throw new InvalidOperationException($"Missing configuration section: {kurrentSectionPath}");

                ValidateSourceSettings(sourceSettings, kurrentSectionPath);

                return sourceSettings;
            });

        services.AddSingleton(
            provider =>
            {
                var connectionString = configuration.GetConnectionString(KurrentDbConnectionStringName);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    throw new InvalidOperationException($"Missing connection string: {KurrentDbConnectionStringName}");
                }

                var connectionSettings = KurrentDBClientSettings.Create(connectionString);

                return new KurrentDBClient(connectionSettings);
            });

        services.AddHealthChecks()
                .AddCheck<KurrentDbHealthCheck>("KurrentDB", HealthStatus.Unhealthy);

        services.AddSingleton<IEventModelIdResolver>(
            provider =>
            {
                var projectionSettings = provider.GetRequiredService<IProjectionSettings<TState>>();
                var logger = provider.GetRequiredService<ILogger<EventModelIdResolver>>();
                return new EventModelIdResolver(logger, projectionSettings.ModelIdResolutionStrategy);
            });

        services.AddSingleton<IEventEnvelopeFactory, EventEnvelopeFactory>();

        services.AddSingleton<IEventSource<EventEnvelope>, KurrentDbEventSource<TState>>();

        return services;
    }

    private static void ValidateSourceSettings(
        KurrentDbSourceSettings sourceSettings,
        string sectionPath)
    {
        if (sourceSettings.SubscriptionBufferCapacity < 1)
        {
            throw new InvalidOperationException($"{sectionPath}:SubscriptionBufferCapacity must be greater than or equal to 1.");
        }

        ArgumentNullException.ThrowIfNull(sourceSettings.Filter);

        if (sourceSettings.Filter.Kind != KurrentDbFilterKind.StreamPrefix)
        {
            throw new InvalidOperationException($"{sectionPath}:Filter:Kind supports only '{KurrentDbFilterKind.StreamPrefix}' in the current implementation.");
        }

        _ = KurrentDbSubscriptionFilterFactory.Create(sourceSettings.Filter);
    }
}
