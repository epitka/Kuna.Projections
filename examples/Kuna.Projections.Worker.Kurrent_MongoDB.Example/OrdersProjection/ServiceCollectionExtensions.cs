using System.Diagnostics.CodeAnalysis;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Core;
using Kuna.Projections.Sink.MongoDB;
using Kuna.Projections.Source.KurrentDB;
using Kuna.Projections.Worker.Kurrent_MongoDB.Example.OrdersProjection.Events;
using Kuna.Projections.Worker.Kurrent_MongoDB.Example.OrdersProjection.Model;
using Serilog;

namespace Kuna.Projections.Worker.Kurrent_MongoDB.Example.OrdersProjection;

/// <summary>
/// Extensions for the IServiceCollection
/// </summary>
[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddOrdersProjections(this IServiceCollection services, IConfiguration configuration)
    {
        var mongoDbConnectionString = configuration.GetConnectionString("MongoDB");

        if (string.IsNullOrWhiteSpace(mongoDbConnectionString))
        {
            throw new InvalidOperationException("Missing connection string: MongoDB");
        }

        var factory = LoggerFactory.Create(
            builder =>
            {
                builder.AddSerilog();
            });

        services.AddKurrentDBSource<Order>(configuration, factory, "OrdersProjection");
        services.AddMongoProjectionsDataStore<Order>(
            options =>
            {
                options.ConnectionString = mongoDbConnectionString;
                options.DatabaseName = "orders_projection";
                options.CollectionPrefix = "orders";
            });
        services.AddProjection<Order>(
                    configuration,
                    settingsSectionName: "OrdersProjection")
                .WithInitialEvent<OrderCreatedEvent>();

        return services;
    }
}
