using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddMongoProjectionsDataStore_Should_Register_Required_Services()
    {
        ServiceCollection services = new();

        IServiceCollection returned = services.AddMongoProjectionsDataStore<TestModel>(
            options =>
            {
                options.ConnectionString = "mongodb://localhost:27017";
                options.DatabaseName = "testdb";
            });

        returned.ShouldBeSameAs(services);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IProjectionFailureHandler<TestModel>)
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IModelStateSink<TestModel>)
                          && descriptor.ImplementationFactory != null
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IModelStateStore<TestModel>)
                          && descriptor.ImplementationFactory != null
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IProjectionCheckpointStore<TestModel>)
                          && descriptor.ImplementationFactory != null
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IProjectionStartupTask)
                          && descriptor.ImplementationFactory != null
                          && descriptor.Lifetime == ServiceLifetime.Singleton);
    }
}
