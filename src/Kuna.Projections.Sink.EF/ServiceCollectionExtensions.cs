using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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
    public static IServiceCollection AddSqlProjectionsDataStore<TState, TDataContext>(
        this IServiceCollection services,
        string settingsSectionName,
        string? schema = null)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        var registrationKey = ProjectionRegistration.GetKey<TState>(settingsSectionName);

        services.AddSingleton(new ProjectionSchema<TDataContext>(schema));
        services.AddKeyedSingleton<IProjectionFailureHandler<TState>, ProjectionFailureHandler<TState, TDataContext>>(registrationKey);

        services.AddKeyedSingleton<DataStore<TState, TDataContext>>(
            registrationKey,
            (sp, _) => new DataStore<TState, TDataContext>(
                sp,
                sp.GetRequiredService<IDuplicateKeyExceptionDetector>(),
                sp.GetRequiredKeyedService<IProjectionFailureHandler<TState>>(registrationKey),
                sp.GetRequiredKeyedService<IProjectionSettings<TState>>(registrationKey),
                sp.GetRequiredService<ILogger<DataStore<TState, TDataContext>>>()));

        services.AddKeyedSingleton<IModelStateSink<TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<DataStore<TState, TDataContext>>(registrationKey));

        services.AddKeyedSingleton<IModelStateStore<TState>>(
            registrationKey,
            (sp, _) => sp.GetRequiredKeyedService<DataStore<TState, TDataContext>>(registrationKey));

        services.AddKeyedSingleton<ICheckpointStore>(registrationKey, (sp, _) => sp.GetRequiredKeyedService<DataStore<TState, TDataContext>>(registrationKey));

        services.AddHealthChecks().AddDbContextCheck<TDataContext>();

        return services;
    }

    public static IProjectionRegistrationBuilder<TState> UseSqlDataStore<TState, TDataContext>(
        this IProjectionRegistrationBuilder<TState> builder,
        string? schema = null)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        builder.Services.AddSqlProjectionsDataStore<TState, TDataContext>(builder.SettingsSectionName, schema);
        return builder;
    }
}
