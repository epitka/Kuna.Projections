using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF;

/// <summary>
/// Registers EF Core-backed projection sink, state-store, checkpoint-store, and
/// failure-handler services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the SQL projection persistence services for the specified model
    /// state and DbContext types.
    /// </summary>
    public static IServiceCollection AddSqlProjectionsDataStore<TState, TDataContext>(this IServiceCollection services, string schema)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        services.AddSingleton(new ProjectionSchema<TDataContext>(schema));
        services.AddSingleton<IProjectionFailureHandler<TState>, ProjectionFailureHandler<TState, TDataContext>>();

        services.AddSingleton<DataStore<TState, TDataContext>>();
        services.AddSingleton<IModelStateSink<TState>>(sp => sp.GetRequiredService<DataStore<TState, TDataContext>>());
        services.AddSingleton<IProjectionStoreWriter<TState>>(sp => sp.GetRequiredService<DataStore<TState, TDataContext>>());
        services.AddSingleton<IModelStateStore<TState>>(sp => sp.GetRequiredService<DataStore<TState, TDataContext>>());
        services.AddSingleton<ICheckpointStore>(sp => sp.GetRequiredService<DataStore<TState, TDataContext>>());

        services.AddHealthChecks().AddDbContextCheck<TDataContext>();

        return services;
    }
}
