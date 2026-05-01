using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF.MySql;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddMySqlDuplicateKeyDetection()
        {
            services.AddSingleton<IDuplicateKeyExceptionDetector, MySqlDuplicateKeyExceptionDetector>();
            return services;
        }
        public IServiceCollection AddMySqlProjectionsDataStore<TState, TDataContext>(string? schema = null)
            where TState : class, IModel, new()
            where TDataContext : DbContext, IProjectionDbContext
        {
            services.AddMySqlDuplicateKeyDetection();
            services.AddSqlProjectionsDataStore<TState, TDataContext>(schema);
            return services;
        }
    }
}
