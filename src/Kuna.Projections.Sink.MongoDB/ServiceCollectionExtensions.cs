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
        Action<MongoProjectionOptions> configure)
        where TState : class, IModel, new()
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        MongoProjectionOptions options = new();
        configure(options);
        MongoProjectionOptionsValidator.Validate(options);
        MongoSerializationRegistry.EnsureInitialized();

        services.AddSingleton(options);
        services.AddSingleton<CollectionNamer>();
        services.AddSingleton<IMongoClient>(_ => new MongoClient(options.ConnectionString));
        services.AddSingleton(sp => sp.GetRequiredService<IMongoClient>().GetDatabase(options.DatabaseName));
        services.AddSingleton<ModelDataStore<TState>>();
        services.AddSingleton<ProjectionCheckpointStore<TState>>();
        services.AddSingleton<MongoIndexesInitializer<TState>>();
        services.AddSingleton<ICheckpointStore>(sp => sp.GetRequiredService<ProjectionCheckpointStore<TState>>());
        services.AddSingleton<IProjectionStartupTask>(sp => sp.GetRequiredService<MongoIndexesInitializer<TState>>());
        services.AddSingleton<IProjectionFailureHandler<TState>, ProjectionFailureHandler<TState>>();
        services.AddSingleton<IModelStateSink<TState>>(sp => sp.GetRequiredService<ModelDataStore<TState>>());
        services.AddSingleton<IModelStateStore<TState>>(sp => sp.GetRequiredService<ModelDataStore<TState>>());
        services.AddSingleton<IProjectionCheckpointStore<TState>>(
            sp => new ProjectionCheckpointStoreAdapter<TState>(sp.GetRequiredService<ICheckpointStore>()));

        return services;
    }
}
