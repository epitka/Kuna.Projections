using System.Reflection;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Core;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddProjectionHost(this IServiceCollection services, Assembly assembly)
    {
        services.AddSingleton<ProjectionHostWorker>(
            serviceProvider =>
            {
                var startupTasks = serviceProvider.GetServices<IProjectionStartupTask>().ToArray();
                var pipelines = serviceProvider.GetServices<IProjectionPipeline>().ToArray();

                return new ProjectionHostWorker(
                    serviceProvider.GetRequiredService<IHostApplicationLifetime>(),
                    serviceProvider.GetRequiredService<ILogger<ProjectionHostWorker>>(),
                    startupTasks,
                    pipelines);
            });

        services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<ProjectionHostWorker>());

        return services;
    }

    public static ProjectionRegistrationBuilder<TState> AddProjection<TState>(
        this IServiceCollection services,
        IConfiguration configuration,
        Action<ProjectionSettings<TState>>? configureProjection = null,
        string? settingsSectionName = null)
        where TState : class, IModel, new()
    {
        var exportedTypes = typeof(TState).Assembly.GetExportedTypes();

        services.AddSingleton<IProjectionFactory<TState>>(
            sp =>
            {
                var stateStore = sp.GetRequiredService<IModelStateStore<TState>>();

                var projectionType = exportedTypes
                    .Single(x => x.BaseType == typeof(Projection<TState>));

                var projectionCtorFunc = projectionType
                    .CreateConstructorFunc(new[] { typeof(Guid), });

                if (projectionCtorFunc == null)
                {
                    throw new InvalidOperationException($"Projection type {projectionType.FullName} does not have a constructor with Guid parameter.");
                }

                return new ProjectionFactory<TState>(
                    modelId => (Projection<TState>)projectionCtorFunc.Invoke(new object[] { modelId, }),
                    stateStore);
            });

        services.AddSingleton<ProjectionEngine<TState>>();
        services.AddSingleton<IModelStateTransformer<EventEnvelope, TState>>(sp => sp.GetRequiredService<ProjectionEngine<TState>>());
        services.AddSingleton<IProjectionLifecycle>(sp => sp.GetRequiredService<ProjectionEngine<TState>>());
        services.AddSingleton<IModelStateCache<TState>, InMemoryModelStateCache<TState>>();

        services.AddSingleton<IProjectionPipeline<TState>>(
            sp => new ProjectionPipeline<EventEnvelope, TState>(
                sp.GetRequiredService<IEventSource<EventEnvelope>>(),
                sp.GetRequiredService<IModelStateTransformer<EventEnvelope, TState>>(),
                sp.GetRequiredService<IProjectionLifecycle>(),
                sp.GetRequiredService<IModelStateCache<TState>>(),
                sp.GetRequiredService<IModelStateSink<TState>>(),
                sp.GetRequiredService<ICheckpointStore>(),
                sp.GetRequiredService<IProjectionSettings<TState>>(),
                sp.GetRequiredService<ILogger<ProjectionPipeline<EventEnvelope, TState>>>()));

        services.AddSingleton<IProjectionPipeline>(sp => sp.GetRequiredService<IProjectionPipeline<TState>>());

        var resolvedSettingsSectionName = string.IsNullOrWhiteSpace(settingsSectionName)
                                              ? ProjectionSettingsSection.Name
                                              : settingsSectionName;

        var projectionSettings = configuration
                                 .GetRequiredSection(resolvedSettingsSectionName)
                                 .Get<ProjectionSettings<TState>>();

        projectionSettings ??= new ProjectionSettings<TState>();

        configureProjection?.Invoke(projectionSettings);

        services.AddSingleton<IProjectionSettings<TState>>(projectionSettings);
        services.TryAddSingleton(new ProjectionCreationRegistration<TState>(initialEventType: null));

        return new ProjectionRegistrationBuilder<TState>(services);
    }
}
