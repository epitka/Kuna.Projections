using System.Reflection;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using KurrentDB.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Registers the Kurrent-backed event source, health checks, and supporting
/// services required to feed projection envelopes into the core pipeline.
/// </summary>
public static class ServiceCollectionExtensions
{
    private const string EventStoreConnectionStringName = "EventStore";

    /// <summary>
    /// Adds the Kurrent source implementation and related configuration for the
    /// specified projection model state type.
    /// </summary>
    public static IServiceCollection AddEventStoreSource<TState>(
        this IServiceCollection services,
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        string? settingsSectionPath = null)
        where TState : class, IModel, new()
    {
        var assembly = Assembly.GetEntryAssembly();
        var exportedTypes = assembly!.GetExportedTypes();
        var resolvedSettingsSectionPath = string.IsNullOrWhiteSpace(settingsSectionPath)
            ? EventStoreSourceSettings.SectionName
            : settingsSectionPath;

        services.AddSingleton<IEventDeserializer>(
            provider =>
            {
                var eventTypes = exportedTypes
                                 .Where(x => x.IsSubclassOf(typeof(Event)))
                                 .ToArray();

                return new EventDeserializer(eventTypes, loggerFactory.CreateLogger<EventDeserializer>());
            });

        services.AddSingleton(
            provider =>
            {
                var sourceSettings = configuration
                                     .GetRequiredSection(resolvedSettingsSectionPath)
                                     .Get<EventStoreSourceSettings>()
                                     ?? throw new InvalidOperationException($"Missing configuration section: {resolvedSettingsSectionPath}");

                if (string.IsNullOrWhiteSpace(sourceSettings.StreamName))
                {
                    throw new InvalidOperationException($"Missing required configuration value: {resolvedSettingsSectionPath}:StreamName");
                }

                return sourceSettings;
            });

        services.AddSingleton(
            provider =>
            {
                var connectionString = configuration.GetConnectionString(EventStoreConnectionStringName);

                if (string.IsNullOrWhiteSpace(connectionString))
                {
                    throw new InvalidOperationException($"Missing connection string: {EventStoreConnectionStringName}");
                }

                var connectionSettings = KurrentDBClientSettings.Create(connectionString);

                return new KurrentDBClient(connectionSettings);
            });

        services.AddHealthChecks()
                .AddCheck<EventStoreHealthCheck>("EventStore", HealthStatus.Unhealthy);

        services.AddSingleton<IEventModelIdResolver>(
            provider =>
            {
                var sourceSettings = provider.GetRequiredService<EventStoreSourceSettings>();
                var logger = provider.GetRequiredService<ILogger<EventModelIdResolver>>();
                return new EventModelIdResolver(logger, sourceSettings.ModelIdResolutionStrategy);
            });

        services.AddSingleton<IEventEnvelopeFactory, EventEnvelopeFactory>();

        services.AddSingleton<IEventSource<EventEnvelope>, EventStoreEventSource<TState>>();

        return services;
    }
}
