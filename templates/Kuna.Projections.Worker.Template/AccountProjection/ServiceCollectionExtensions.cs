using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core;
using Kuna.Projections.Sink.EF;
using Kuna.Projections.Source.Kurrent;
using Kuna.Projections.Worker.Template.AccountProjection.Model;
using Microsoft.EntityFrameworkCore;
using Serilog;

namespace Kuna.Projections.Worker.Template.AccountProjection;

public static class ServiceCollectionExtensions
{
    private const string ProjectionSchema = "account_projection";

    public static IServiceCollection AddAccountProjection(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IProjectionStartupTask, AccountProjectionStartupTask>();

        var postgreSqlConnectionString = configuration.GetConnectionString("PostgreSql");

        if (string.IsNullOrWhiteSpace(postgreSqlConnectionString))
        {
            throw new InvalidOperationException("Missing connection string: PostgreSql");
        }

        services.AddDbContext<AccountProjectionDbContext>(
            options =>
            {
                options.UseNpgsql(
                    postgreSqlConnectionString,
                    npgsqlOptions =>
                    {
                        npgsqlOptions.EnableRetryOnFailure(
                            maxRetryCount: 3,
                            maxRetryDelay: TimeSpan.FromSeconds(1),
                            errorCodesToAdd: null);
                    });
            });

        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
                builder.AddSerilog();
            });

        services.AddEventStoreSource<Account>(configuration, loggerFactory, "AccountProjection:EventStoreSource");
        services.AddSqlProjectionsDataStore<Account, AccountProjectionDbContext>(schema: ProjectionSchema);
        services.AddProjection<Account>(configuration, settingsSectionName: "AccountProjection");

        return services;
    }
}
