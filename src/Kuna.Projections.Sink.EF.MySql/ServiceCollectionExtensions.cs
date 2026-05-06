using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF.MySql;

public static class ServiceCollectionExtensions
{
    public static IProjectionRegistrationBuilder<TState> UseMySqlDataStore<TState, TDataContext>(
        this IProjectionRegistrationBuilder<TState> builder,
        string? schema = null)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        builder.Services.AddMySqlProjectionsDataStore<TState, TDataContext>(builder.SettingsSectionName, schema);
        return builder;
    }

    extension(IServiceCollection services)
    {
        public IServiceCollection AddMySqlDuplicateKeyDetection()
        {
            services.AddSingleton<IDuplicateKeyExceptionDetector, MySqlDuplicateKeyExceptionDetector>();
            return services;
        }
        public IServiceCollection AddMySqlProjectionsDataStore<TState, TDataContext>(string settingsSectionName, string? schema = null)
            where TState : class, IModel, new()
            where TDataContext : DbContext, IProjectionDbContext
        {
            services.AddMySqlDuplicateKeyDetection();
            services.AddSqlProjectionsDataStore<TState, TDataContext>(settingsSectionName, schema);
            return services;
        }
    }
}
