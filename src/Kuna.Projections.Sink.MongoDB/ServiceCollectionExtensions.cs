using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.MongoDB;

/// <summary>
/// Registers MongoDB-backed projection sink, state-store, checkpoint-store, and
/// failure-handler services.
/// </summary>
public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        /// <summary>
        /// Adds the MongoDB projection persistence services for the specified model state type.
        /// </summary>
        public IServiceCollection AddMongoProjectionsDataStore<TState>(
            string connectionString,
            string databaseName,
            Action<ProjectionOptions>? configure = null)
            where TState : class, IModel, new()
        {
            return services.AddMongoProjectionsDataStore<TState>(
                connectionString,
                databaseName,
                configure,
                options => new CollectionNamer(options));
        }
        /// <summary>
        /// Adds the MongoDB projection persistence services for the specified model state type
        /// using a caller-supplied collection namer implementation.
        /// </summary>
        public IServiceCollection AddMongoProjectionsDataStore<TState>(
            string connectionString,
            string databaseName,
            Action<ProjectionOptions>? configure,
            Func<ProjectionOptions, ICollectionNamer> collectionNamerFactory)
            where TState : class, IModel, new()
        {
            ArgumentNullException.ThrowIfNull(services);
            ArgumentNullException.ThrowIfNull(collectionNamerFactory);

            ProjectionOptions options = new(connectionString, databaseName);
            configure?.Invoke(options);
            SerializerRegistry.EnsureInitialized();

            var collectionNamer = collectionNamerFactory(options);
            ArgumentNullException.ThrowIfNull(collectionNamer);

            services.AddSingleton(new ProjectionContext<TState>(options, collectionNamer));
            services.AddSingleton<ModelDataStore<TState>>();
            services.AddSingleton<ProjectionCheckpointStore<TState>>();
            services.AddSingleton<ProjectionStartupTask<TState>>();
            services.AddSingleton<ICheckpointStore>(sp => sp.GetRequiredService<ProjectionCheckpointStore<TState>>());
            services.AddSingleton<IProjectionStartupTask>(sp => sp.GetRequiredService<ProjectionStartupTask<TState>>());
            services.AddSingleton<IProjectionFailureHandler<TState>, ProjectionFailureHandler<TState>>();
            services.AddSingleton<IModelStateSink<TState>>(sp => sp.GetRequiredService<ModelDataStore<TState>>());
            services.AddSingleton<IModelStateStore<TState>>(sp => sp.GetRequiredService<ModelDataStore<TState>>());
            services.AddSingleton<IProjectionCheckpointStore<TState>>(
                sp => new ProjectionCheckpointStoreAdapter<TState>(sp.GetRequiredService<ProjectionCheckpointStore<TState>>()));

            return services;
        }
    }
}
