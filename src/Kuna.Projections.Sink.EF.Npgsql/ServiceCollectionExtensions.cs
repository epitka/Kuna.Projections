using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF.Npgsql;

public static class ServiceCollectionExtensions
{
    public static IProjectionRegistrationBuilder<TState> UseNpgsqlDataStore<TState, TDataContext>(
        this IProjectionRegistrationBuilder<TState> builder,
        string? schema = null)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        builder.Services.AddNpgsqlProjectionsDataStore<TState, TDataContext>(builder.SettingsSectionName, schema);
        return builder;
    }

    extension(IServiceCollection services)
    {
        public IServiceCollection AddNpgsqlDuplicateKeyDetection()
        {
            services.AddSingleton<IDuplicateKeyExceptionDetector, NpgsqlDuplicateKeyExceptionDetector>();
            return services;
        }
        public IServiceCollection AddNpgsqlProjectionsDataStore<TState, TDataContext>(string settingsSectionName, string? schema = null)
            where TState : class, IModel, new()
            where TDataContext : DbContext, IProjectionDbContext
        {
            services.AddNpgsqlDuplicateKeyDetection();
            services.AddSqlProjectionsDataStore<TState, TDataContext>(settingsSectionName, schema);
            return services;
        }
    }
}
