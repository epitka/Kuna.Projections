using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Sink.EF.Npgsql;

public static class ServiceCollectionExtensions
{
    extension(IServiceCollection services)
    {
        public IServiceCollection AddNpgsqlDuplicateKeyDetection()
        {
            services.AddSingleton<IDuplicateKeyExceptionDetector, NpgsqlDuplicateKeyExceptionDetector>();
            return services;
        }
        public IServiceCollection AddNpgsqlProjectionsDataStore<TState, TDataContext>(string? schema = null)
            where TState : class, IModel, new()
            where TDataContext : DbContext, IProjectionDbContext
        {
            services.AddNpgsqlDuplicateKeyDetection();
            services.AddSqlProjectionsDataStore<TState, TDataContext>(schema);
            return services;
        }
    }
}
