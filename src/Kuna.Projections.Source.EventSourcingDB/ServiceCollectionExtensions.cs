using System.Reflection;
using EventSourcingDb;
using EventSourcingDb.DependencyInjection;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Registers the EventSourcingDB-backed event source, health checks, and supporting
/// services required to feed projection envelopes into the core pipeline.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the EventSourcingDB source implementation and related configuration for
    /// the specified projection model state type.
    /// </summary>
    /// <param name="eventTypeNameResolver">
    /// Optional resolver that maps the CloudEvent <c>type</c> to the CLR type name
    /// used for lookup. Defaults to taking the segment after the last dot, e.g.
    /// <c>orders.OrderCreated</c> resolves to <c>OrderCreated</c>.
    /// </param>
    public static IServiceCollection AddEventSourcingDbSource<TState>(
        this IServiceCollection services,
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        string settingsSectionName,
        Func<string, string>? eventTypeNameResolver = null)
        where TState : class, IModel, new()
    {
        var assembly = Assembly.GetEntryAssembly();
        var eventTypes = ResolveEventTypes(assembly, typeof(TState).Assembly);
        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);

        services.AddSingleton<IEventDeserializer>(
            provider => new EventDeserializer(
                eventTypes,
                eventTypeNameResolver,
                loggerFactory.CreateLogger<EventDeserializer>()));

        services.TryAddSingleton<ICheckpointSerializer<string>, EventSourcingDbCheckpointSerializer>();

        if (services.All(descriptor => descriptor.ServiceType != typeof(IClient)))
        {
            services.AddEventSourcingDb(configuration);
        }

        services.AddHealthChecks()
                .AddCheck<EventSourcingDbHealthCheck>("EventSourcingDB", HealthStatus.Unhealthy);

        services.AddKeyedSingleton<IEventSource<EventEnvelope>>(
            registrationKey,
            (provider, _) =>
            {
                var resolvedProjectionSettings = provider.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey);

                var sourceSettings = ResolveSourceSettings(configuration, settingsSectionName);

                var modelIdResolver = new EventSourcingDbModelIdResolver(
                    provider.GetRequiredService<ILogger<EventSourcingDbModelIdResolver>>(),
                    resolvedProjectionSettings.ModelIdResolutionStrategy,
                    sourceSettings.ModelIdSubjectSegmentIndex);

                var envelopeFactory = new EventEnvelopeFactory(
                    provider.GetRequiredService<IEventDeserializer>(),
                    modelIdResolver);

                var headPositionReader = new EventSourcingDbHeadPositionReader(
                    provider.GetRequiredService<IClient>(),
                    sourceSettings.Subject,
                    sourceSettings.Recursive);

                var source = new EventSourcingDbEventSource<TState>(
                    provider.GetRequiredService<IClient>(),
                    envelopeFactory,
                    provider.GetRequiredService<ICheckpointSerializer<string>>(),
                    headPositionReader,
                    sourceSettings,
                    resolvedProjectionSettings,
                    provider.GetRequiredService<ILogger<EventSourcingDbEventSource<TState>>>());

                return source;
            });

        return services;
    }

    /// <summary>
    /// Registers the EventSourcingDB source for the projection under construction.
    /// </summary>
    public static IProjectionRegistrationBuilder<TState> UseEventSourcingDbSource<TState>(
        this IProjectionRegistrationBuilder<TState> builder,
        ILoggerFactory loggerFactory,
        Func<string, string>? eventTypeNameResolver = null)
        where TState : class, IModel, new()
    {
        builder.Services.AddEventSourcingDbSource<TState>(
            builder.Configuration,
            loggerFactory,
            builder.SettingsSectionName,
            eventTypeNameResolver);

        return builder;
    }

    private static EventSourcingDbSourceSettings ResolveSourceSettings(
        IConfiguration configuration,
        string settingsSectionName)
    {
        var sectionPath = $"{settingsSectionName}:{EventSourcingDbSourceSettings.SectionName}";
        var section = configuration.GetSection(sectionPath);

        if (!section.Exists())
        {
            return new EventSourcingDbSourceSettings();
        }

        return section.Get<EventSourcingDbSourceSettings>() ?? new EventSourcingDbSourceSettings();
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
}
