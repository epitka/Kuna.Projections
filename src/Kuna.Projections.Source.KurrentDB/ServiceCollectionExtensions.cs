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

namespace Kuna.Projections.Source.KurrentDB;

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
        string settingsSectionName)
        where TState : class, IModel, new()
    {
        var assembly = Assembly.GetEntryAssembly();
        var eventTypes = ResolveEventTypes(assembly, typeof(TState).Assembly);
        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);

        services.AddSingleton<IEventDeserializer>(
            provider =>
            {
                return new EventDeserializer(eventTypes, loggerFactory.CreateLogger<EventDeserializer>());
            });

        services.TryAddSingleton<ICheckpointSerializer<Position>, KurrentDbCheckpointSerializer>();

        services.TryAddSingleton(
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

        services.AddKeyedSingleton<IProjectionEventSource<TState>>(
            registrationKey,
            (provider, _) =>
            {
                var resolvedProjectionSettings = provider.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey);

                if (resolvedProjectionSettings.Source != ProjectionSourceKind.KurrentDB)
                {
                    throw new InvalidOperationException(
                        $"Unsupported projection source '{resolvedProjectionSettings.Source}' for section '{settingsSectionName}'.");
                }

                var kurrentSectionPath = $"{settingsSectionName}:{KurrentDbSourceSettings.SectionName}";
                var kurrentSection = configuration.GetSection(kurrentSectionPath);

                if (!kurrentSection.Exists())
                {
                    throw new InvalidOperationException($"Missing required configuration section: {kurrentSectionPath}");
                }

                var sourceSettings = kurrentSection.Get<KurrentDbSourceSettings>()
                                     ?? throw new InvalidOperationException($"Missing configuration section: {kurrentSectionPath}");

                ValidateSourceSettings(sourceSettings, kurrentSectionPath);

                var modelIdResolver = new EventModelIdResolver(
                    provider.GetRequiredService<ILogger<EventModelIdResolver>>(),
                    resolvedProjectionSettings.ModelIdResolutionStrategy);

                var envelopeFactory = new EventEnvelopeFactory(
                    provider.GetRequiredService<IEventDeserializer>(),
                    modelIdResolver);

                var source = new KurrentDbEventSource<TState>(
                    provider.GetRequiredService<KurrentDBClient>(),
                    envelopeFactory,
                    provider.GetRequiredService<ICheckpointSerializer<Position>>(),
                    sourceSettings,
                    resolvedProjectionSettings,
                    provider.GetRequiredService<ILogger<KurrentDbEventSource<TState>>>());

                return new ProjectionEventSource<TState>(source);
            });

        return services;
    }
    public static IProjectionRegistrationBuilder<TState> UseKurrentDbSource<TState>(
        this IProjectionRegistrationBuilder<TState> builder,
        ILoggerFactory loggerFactory)
        where TState : class, IModel, new()
    {
        builder.Services.AddKurrentDBSource<TState>(builder.Configuration, loggerFactory, builder.SettingsSectionName);
        return builder;
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
                    // needs the assemblies that can be loaded in the current app.
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

    private static void ValidateSourceSettings(
        KurrentDbSourceSettings sourceSettings,
        string sectionPath)
    {
        ArgumentNullException.ThrowIfNull(sourceSettings.Filter);

        _ = KurrentDbSubscriptionFilterFactory.Create(sourceSettings.Filter);
    }
}
