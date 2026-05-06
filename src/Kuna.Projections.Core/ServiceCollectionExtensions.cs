using System.Reflection;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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
        string settingsSectionName,
        Action<ProjectionSettings<TState>>? configureProjection = null)
        where TState : class, IModel, new()
    {
        var exportedTypes = typeof(TState).Assembly.GetExportedTypes();
        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);

        services.AddKeyedSingleton<IProjectionFactory<TState>>(
            registrationKey,
            (sp, _) =>
            {
                var stateStore = sp.GetRequiredKeyedService<IModelStateStore<TState>>(registrationKey);

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

        services.AddKeyedSingleton<ProjectionEngine<TState>>(
            registrationKey,
            (sp, _) => new ProjectionEngine<TState>(
                sp.GetRequiredKeyedService<IProjectionFactory<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionFailureHandler<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IModelStateCache<TState>>(registrationKey),
                sp.GetRequiredKeyedService<ProjectionCreationRegistration<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey),
                sp.GetRequiredService<ILogger<ProjectionEngine<TState>>>()));
        services.AddKeyedSingleton<IModelStateTransformer<EventEnvelope, TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<ProjectionEngine<TState>>(registrationKey));
        services.AddKeyedSingleton<IProjectionLifecycle<TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<ProjectionEngine<TState>>(registrationKey));
        services.AddKeyedSingleton<IModelStateCache<TState>>(
            registrationKey,
            (sp, _) => new InMemoryModelStateCache<TState>(sp.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey)));

        services.AddKeyedSingleton<IProjectionPipeline<TState>>(
            registrationKey,
            (sp, _) => new ProjectionPipeline<EventEnvelope, TState>(
                sp.GetRequiredKeyedService<IProjectionEventSource<TState>>(registrationKey).Value,
                sp.GetRequiredKeyedService<IModelStateTransformer<EventEnvelope, TState>>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionLifecycle<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IModelStateCache<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IModelStateSink<TState>>(registrationKey),
                sp.GetRequiredKeyedService<ICheckpointStore>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey),
                sp.GetRequiredService<ILogger<ProjectionPipeline<EventEnvelope, TState>>>()));

        services.AddSingleton<IProjectionPipeline>(sp => sp.GetRequiredKeyedService<IProjectionPipeline<TState>>(registrationKey));

        var projectionSettings = configuration
                                 .GetRequiredSection(settingsSectionName)
                                 .Get<ProjectionSettings<TState>>();

        projectionSettings ??= new ProjectionSettings<TState>();

        configureProjection?.Invoke(projectionSettings);

        if (string.IsNullOrWhiteSpace(projectionSettings.InstanceId))
        {
            throw new InvalidOperationException(
                $"Projection settings section '{settingsSectionName}' must define a non-empty '{nameof(IProjectionSettings<TState>.InstanceId)}'.");
        }

        services.AddKeyedSingleton<IProjectionSettings<TState>>(registrationKey, projectionSettings);

        return new ProjectionRegistrationBuilder<TState>(services, configuration, settingsSectionName, registrationKey);
    }
}
