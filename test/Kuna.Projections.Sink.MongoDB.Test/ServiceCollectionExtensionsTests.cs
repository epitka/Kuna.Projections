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

        var returned = services.AddMongoProjectionsDataStore<TestModel>(
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

    [Fact]
    public void AddMongoProjectionsDataStore_Should_Isolate_Typed_Dependencies_For_Multiple_Models()
    {
        ServiceCollection services = new();

        services.AddMongoProjectionsDataStore<TestModel>(
            options =>
            {
                options.ConnectionString = "mongodb://localhost:27017";
                options.DatabaseName = "testdb";
                options.CollectionPrefix = "orders";
            });

        services.AddMongoProjectionsDataStore<SecondaryTestModel>(
            options =>
            {
                options.ConnectionString = "mongodb://localhost:27017";
                options.DatabaseName = "testdb";
                options.CollectionPrefix = "invoices";
            });

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IModelStateStore<TestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IModelStateStore<SecondaryTestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionFailureHandler<TestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionFailureHandler<SecondaryTestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionCheckpointStore<TestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionCheckpointStore<SecondaryTestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionCheckpointStore<TestModel>>()
                .Value
                .ShouldNotBeSameAs(provider.GetRequiredService<IProjectionCheckpointStore<SecondaryTestModel>>().Value);

        provider.GetServices<IProjectionStartupTask>().Count().ShouldBe(2);
    }
}
