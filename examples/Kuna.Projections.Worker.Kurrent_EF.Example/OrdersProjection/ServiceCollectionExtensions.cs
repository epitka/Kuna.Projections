using System.Diagnostics.CodeAnalysis;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core;
using Kuna.Projections.Sink.EF;
using Kuna.Projections.Sink.EF.Data;
using Kuna.Projections.Source.Kurrent;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;
using Microsoft.EntityFrameworkCore;
using Serilog;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;

/// <summary>
/// Extensions for the IServiceCollection
/// </summary>
[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    private const string ProjectionSchema = "dbo";

    public static IServiceCollection AddOrdersProjections(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddSingleton<IProjectionStartupTask, OrdersProjectionStartupTask>();

        var postgreSqlConnectionString = configuration.GetConnectionString("PostgreSql");

        if (string.IsNullOrWhiteSpace(postgreSqlConnectionString))
        {
            throw new InvalidOperationException("Missing connection string: PostgreSql");
        }

        services.AddDbContext<OrdersDbContext>(
            options =>
            {
                options.UseNpgsql(
                           postgreSqlConnectionString,
                           npgsqlOptionsAction: npgsqlOptions =>
                           {
                               npgsqlOptions.EnableRetryOnFailure(
                                   maxRetryCount: 3,
                                   maxRetryDelay: TimeSpan.FromSeconds(1),
                                   errorCodesToAdd: null);
                           })
                       .EnableSensitiveDataLogging();
            });

        var factory = LoggerFactory.Create(
            builder =>
            {
                builder.AddSerilog();
            });

        services.AddKurrentDBSource<Order>(configuration, factory, "OrdersProjection");
        services.AddSqlProjectionsDataStore<Order, OrdersDbContext>(schema: ProjectionSchema);
        services.AddProjection<Order>(
            configuration,
            settingsSectionName: "OrdersProjection");

        services.AddScoped<OrdersReplayConsistencyDiagnostics>();

        return services;
    }
}
