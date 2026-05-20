using System.Diagnostics.CodeAnalysis;
using Kuna.Examples.Projections.Orders.Model;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core;
using Kuna.Projections.Sink.MongoDB;
using Kuna.Projections.Source.Kafka;
using Serilog;

namespace Kuna.Projections.Worker.Kafka_MongoDB.Example.OrdersProjection;

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

        services.AddProjection<Order>(
                    configuration,
                    settingsSectionName: "OrdersProjection")
                .UseKafkaSource(factory)
                .UseMongoDataStore(
                    mongoDbConnectionString,
                    "orders_projection",
                    options =>
                    {
                        options.CollectionPrefix = "orders";
                    });

        var registrationKey = ProjectionRegistration.GetKey<Order>("OrdersProjection");
        services.AddScoped(
            provider => new OrdersKafkaStatusDiagnostics(
                provider.GetRequiredKeyedService<ICheckpointStore>(registrationKey),
                provider.GetRequiredService<IConsumerFactory>(),
                provider.GetRequiredService<ICheckpointSerializer<Checkpoint>>(),
                configuration));

        services.AddScoped<OrdersKafkaReplayConsistencyDiagnostics>();

        return services;
    }
}
