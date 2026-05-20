using System.Reflection;
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
        var assembly = Assembly.GetEntryAssembly();
        var eventTypes = ResolveEventTypes(assembly, typeof(TState).Assembly);
        var sourceSettings = ResolveSourceSettings(configuration, settingsSectionName);
        services.TryAddSingleton<ICheckpointSerializer<KafkaCheckpointDocument>, KafkaCheckpointSerializer>();
        services.TryAddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
        services.TryAddSingleton<IKafkaEventDeserializer>(
            provider => new KafkaEventDeserializer(
                eventTypes,
                loggerFactory.CreateLogger<KafkaEventDeserializer>()));

        services.AddSingleton(
            new KafkaHealthCheckRegistration
            {
                SettingsSectionName = settingsSectionName,
                SourceSettings = sourceSettings,
            });

        services.AddHealthChecks()
                .AddCheck<KafkaHealthCheck>("Kafka", HealthStatus.Unhealthy);

        services.TryAddKeyedSingleton<IKafkaSourceTransformer, KunaKafkaSourceTransformer>(registrationKey);

        services.AddKeyedSingleton<IProjectionEventSource<TState>>(
            registrationKey,
            (provider, _) =>
            {
                var projectionSettings = provider.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey);

                if (projectionSettings.Source != ProjectionSourceKind.Kafka)
                {
                    throw new InvalidOperationException($"Unsupported projection source '{projectionSettings.Source}' for section '{settingsSectionName}'.");
                }

                return new ProjectionEventSource<TState>(
                    new KafkaEventSource<TState>(
                        provider.GetRequiredService<IKafkaConsumerFactory>(),
                        provider.GetRequiredKeyedService<IKafkaSourceTransformer>(registrationKey),
                        new KafkaEventEnvelopeFactory(provider.GetRequiredService<IKafkaEventDeserializer>()),
                        provider.GetRequiredService<ICheckpointSerializer<KafkaCheckpointDocument>>(),
                        sourceSettings,
                        projectionSettings,
                        provider.GetRequiredService<ILogger<KafkaEventSource<TState>>>()));
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

    private static KafkaSourceSettings ResolveSourceSettings(
        IConfiguration configuration,
        string settingsSectionName)
    {
        return KafkaSourceSettingsResolver.Resolve(configuration, settingsSectionName);
    }

    private static Type[] ResolveEventTypes(Assembly? entryAssembly, Assembly stateAssembly)
    {
        var candidateAssemblies = ResolveCandidateAssemblies(entryAssembly, stateAssembly);

        return candidateAssemblies
               .SelectMany(SafeGetExportedTypes)
               .Where(x => x.IsSubclassOf(typeof(Event)))
               .Distinct()
               .ToArray();
    }

    private static IReadOnlyCollection<Assembly> ResolveCandidateAssemblies(Assembly? entryAssembly, Assembly stateAssembly)
    {
        var assemblies = new Dictionary<string, Assembly>(StringComparer.Ordinal);
        var toVisit = new Queue<Assembly>();

        if (entryAssembly != null)
        {
            toVisit.Enqueue(entryAssembly);
        }

        toVisit.Enqueue(stateAssembly);

        while (toVisit.Count > 0)
        {
            var assembly = toVisit.Dequeue();

            if (!assemblies.TryAdd(assembly.FullName ?? assembly.GetName().Name ?? assembly.ToString(), assembly))
            {
                continue;
            }

            foreach (var referencedAssembly in assembly.GetReferencedAssemblies())
            {
                try
                {
                    toVisit.Enqueue(Assembly.Load(referencedAssembly));
                }
                catch
                {
                    // Ignore optional or unavailable assemblies. Event discovery only
                    // needs assemblies that can be loaded in the current app.
                }
            }
        }

        return assemblies.Values;
    }

    private static IEnumerable<Type> SafeGetExportedTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetExportedTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(x => x != null).Cast<Type>();
        }
    }
}
