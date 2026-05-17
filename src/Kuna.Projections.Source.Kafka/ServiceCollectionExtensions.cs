using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.Kafka;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaSource<TState>(
        this IServiceCollection services,
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        string settingsSectionName)
        where TState : class, IModel, new()
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentException.ThrowIfNullOrWhiteSpace(settingsSectionName);

        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);
        services.TryAddSingleton<ICheckpointSerializer<KafkaCheckpointDocument>, KafkaCheckpointSerializer>();

        services.AddHealthChecks()
                .AddCheck<KafkaHealthCheck>("Kafka", HealthStatus.Unhealthy);

        services.AddKeyedSingleton<IProjectionEventSource<TState>>(
            registrationKey,
            (provider, _) =>
            {
                var projectionSettings = provider.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey);

                if (projectionSettings.Source != ProjectionSourceKind.Kafka)
                {
                    throw new InvalidOperationException(
                        $"Unsupported projection source '{projectionSettings.Source}' for section '{settingsSectionName}'.");
                }

                var sectionPath = $"{settingsSectionName}:{KafkaSourceSettings.SectionName}";
                var sourceSettings = configuration.GetRequiredSection(sectionPath).Get<KafkaSourceSettings>()
                                     ?? throw new InvalidOperationException($"Missing configuration section: {sectionPath}");

                KafkaSourceSettingsValidator.Validate(sourceSettings, sectionPath);

                return new ProjectionEventSource<TState>(new KafkaEventSource<TState>());
            });

        return services;
    }

    public static IProjectionRegistrationBuilder<TState> UseKafkaSource<TState>(
        this IProjectionRegistrationBuilder<TState> builder,
        ILoggerFactory loggerFactory)
        where TState : class, IModel, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        builder.Services.AddKafkaSource<TState>(builder.Configuration, loggerFactory, builder.SettingsSectionName);
        return builder;
    }
}
