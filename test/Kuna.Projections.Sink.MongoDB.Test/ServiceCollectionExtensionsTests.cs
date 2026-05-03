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
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
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
            descriptor => descriptor.ServiceType == typeof(ICheckpointStore)
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
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
                options.CollectionPrefix = "orders";
            });

        services.AddMongoProjectionsDataStore<SecondaryTestModel>(
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
                options.CollectionPrefix = "invoices";
            });

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IModelStateStore<TestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IModelStateStore<SecondaryTestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionFailureHandler<TestModel>>().ShouldNotBeNull();
        provider.GetRequiredService<IProjectionFailureHandler<SecondaryTestModel>>().ShouldNotBeNull();
        provider.GetServices<ICheckpointStore>().Count().ShouldBe(2);
        provider.GetServices<ICheckpointStore>().Distinct().Count().ShouldBe(2);

        provider.GetServices<IProjectionStartupTask>().Count().ShouldBe(2);
    }

    [Fact]
    public void AddMongoProjectionsDataStore_Should_Use_Custom_Collection_Namer_Factory()
    {
        ServiceCollection services = new();
        var factoryCalled = false;

        services.AddMongoProjectionsDataStore<TestModel>(
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
            },
            options =>
            {
                factoryCalled = true;
                return new CollectionNamer(options);
            });

        factoryCalled.ShouldBeTrue();
    }
}
