using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

/// <summary>
/// Registers MongoDB-backed projection sink, state-store, checkpoint-store, and
/// failure-handler services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MongoDB projection persistence services for the specified model state type.
    /// </summary>
    public static IServiceCollection AddMongoProjectionsDataStore<TState>(
        this IServiceCollection services,
        string settingsSectionName,
        string connectionString,
        string databaseName,
        Action<ProjectionOptions>? configure = null)
        where TState : class, IModel, new()
    {
        return services.AddMongoProjectionsDataStore<TState>(
            settingsSectionName,
            connectionString,
            databaseName,
            configure,
            options => new CollectionNamer(options));
    }

    /// <summary>
    /// Adds the MongoDB projection persistence services for the specified model state type
    /// using a caller-supplied collection namer implementation.
    /// </summary>
    public static IServiceCollection AddMongoProjectionsDataStore<TState>(
        this IServiceCollection services,
        string settingsSectionName,
        string connectionString,
        string databaseName,
        Action<ProjectionOptions>? configure,
        Func<ProjectionOptions, ICollectionNamer> collectionNamerFactory)
        where TState : class, IModel, new()
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(collectionNamerFactory);

        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);
        ProjectionOptions options = new(connectionString, databaseName);
        configure?.Invoke(options);
        ClassMapRegistry.EnsureInitialized<TState>();

        var collectionNamer = collectionNamerFactory(options);
        ArgumentNullException.ThrowIfNull(collectionNamer);

        RegisterMongoClient(services, options.ConnectionString);
        services.AddKeyedSingleton(registrationKey, options);
        services.AddKeyedSingleton<ICollectionNamer>(registrationKey, collectionNamer);
        services.AddKeyedSingleton<IMongoDatabase>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<IMongoClient>(options.ConnectionString)
                         .GetDatabase(options.DatabaseName));

        services.AddKeyedSingleton<ModelDataStore<TState>>(
            registrationKey,
            (sp, _) => new ModelDataStore<TState>(
                sp.GetRequiredKeyedService<IMongoDatabase>(registrationKey),
                sp.GetRequiredKeyedService<ICollectionNamer>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionFailureHandler<TState>>(registrationKey),
                sp.GetKeyedService<IProjectionSettings<TState>>(registrationKey)?.InstanceId ?? settingsSectionName));

        services.AddKeyedSingleton<ProjectionCheckpointStore<TState>>(
            registrationKey,
            (sp, _) => new ProjectionCheckpointStore<TState>(
                sp.GetRequiredKeyedService<IMongoDatabase>(registrationKey),
                sp.GetRequiredKeyedService<ICollectionNamer>(registrationKey)));

        services.AddKeyedSingleton<ProjectionStartupTask<TState>>(
            registrationKey,
            (sp, _) => new ProjectionStartupTask<TState>(
                sp.GetRequiredKeyedService<IMongoDatabase>(registrationKey),
                sp.GetRequiredKeyedService<ICollectionNamer>(registrationKey)));

        services.AddKeyedSingleton<IProjectionFailureHandler<TState>>(
            registrationKey,
            (sp, _) => new ProjectionFailureHandler<TState>(
                sp.GetRequiredKeyedService<IMongoDatabase>(registrationKey),
                sp.GetRequiredKeyedService<ICollectionNamer>(registrationKey)));

        services.AddKeyedSingleton<IModelStateSink<TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<ModelDataStore<TState>>(registrationKey));

        services.AddKeyedSingleton<IModelStateStore<TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<ModelDataStore<TState>>(registrationKey));

        services.AddKeyedSingleton<ICheckpointStore>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<ProjectionCheckpointStore<TState>>(registrationKey));

        services.AddSingleton<IProjectionStartupTask>(sp => sp.GetRequiredKeyedService<ProjectionStartupTask<TState>>(registrationKey));

        return services;
    }

    public static IProjectionRegistrationBuilder<TState> UseMongoDataStore<TState>(
        this IProjectionRegistrationBuilder<TState> builder,
        string connectionString,
        string databaseName,
        Action<ProjectionOptions>? configure = null)
        where TState : class, IModel, new()
    {
        builder.Services.AddMongoProjectionsDataStore<TState>(
            builder.SettingsSectionName,
            connectionString,
            databaseName,
            configure);

        return builder;
    }

    public static IProjectionRegistrationBuilder<TState> UseMongoDataStore<TState>(
        this IProjectionRegistrationBuilder<TState> builder,
        string connectionString,
        string databaseName,
        Action<ProjectionOptions>? configure,
        Func<ProjectionOptions, ICollectionNamer> collectionNamerFactory)
        where TState : class, IModel, new()
    {
        builder.Services.AddMongoProjectionsDataStore<TState>(
            builder.SettingsSectionName,
            connectionString,
            databaseName,
            configure,
            collectionNamerFactory);

        return builder;
    }

    private static void RegisterMongoClient(IServiceCollection services, string connectionString)
    {
        var alreadyRegistered = services.Any(
            descriptor => descriptor.ServiceType == typeof(IMongoClient)
                          && descriptor.IsKeyedService
                          && Equals(descriptor.ServiceKey, connectionString));

        if (alreadyRegistered)
        {
            return;
        }

        services.AddKeyedSingleton<IMongoClient>(
            connectionString,
            (_, _) => new MongoClient(connectionString));
    }
}
