using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF.SqlServer;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        private IServiceCollection AddSqlServerDuplicateKeyDetection()
        {
            services.AddSingleton<IDuplicateKeyExceptionDetector, SqlServerDuplicateKeyExceptionDetector>();
            return services;
        }
        public IServiceCollection AddSqlServerProjectionsDataStore<TState, TDataContext>(string settingsSectionName, string? schema = null)
            where TState : class, IModel, new()
            where TDataContext : DbContext, IProjectionDbContext
        {
            services.AddSqlServerDuplicateKeyDetection();
            services.AddSqlProjectionsDataStore<TState, TDataContext>(settingsSectionName, schema);
            return services;
        }

    }

    public static IProjectionRegistrationBuilder<TState> UseSqlServerDataStore<TState, TDataContext>(this IProjectionRegistrationBuilder<TState> builder, string? schema = null)
        where TState : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        builder.Services.AddSqlServerProjectionsDataStore<TState, TDataContext>(builder.SettingsSectionName, schema);
        return builder;
    }
}
