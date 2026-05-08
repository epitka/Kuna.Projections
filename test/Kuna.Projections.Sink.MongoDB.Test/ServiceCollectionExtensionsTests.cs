using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

public sealed class ServiceCollectionExtensionsTests
{
    private const string OrdersSectionName = "OrdersProjection";
    private const string InvoicesSectionName = "InvoicesProjection";

    [Fact]
    public void AddMongoProjectionsDataStore_Should_Register_Required_Services()
    {
        ServiceCollection services = new();

        var returned = services.AddMongoProjectionsDataStore<TestModel>(
            OrdersSectionName,
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
            });

        returned.ShouldBeSameAs(services);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IProjectionFailureHandler<TestModel>)
                          && descriptor.IsKeyedService
                          && Equals(descriptor.ServiceKey, GetProjectionKey<TestModel>(OrdersSectionName))
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IModelStateSink<TestModel>)
                          && descriptor.IsKeyedService
                          && Equals(descriptor.ServiceKey, GetProjectionKey<TestModel>(OrdersSectionName))
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IModelStateStore<TestModel>)
                          && descriptor.IsKeyedService
                          && Equals(descriptor.ServiceKey, GetProjectionKey<TestModel>(OrdersSectionName))
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(ICheckpointStore)
                          && descriptor.IsKeyedService
                          && Equals(descriptor.ServiceKey, GetProjectionKey<TestModel>(OrdersSectionName))
                          && descriptor.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            descriptor => descriptor.ServiceType == typeof(IProjectionStartupTask)
                          && !descriptor.IsKeyedService
                          && descriptor.Lifetime == ServiceLifetime.Singleton);
    }

    [Fact]
    public void AddMongoProjectionsDataStore_Should_Isolate_Typed_Dependencies_For_Multiple_Models()
    {
        ServiceCollection services = new();

        services.AddMongoProjectionsDataStore<TestModel>(
            OrdersSectionName,
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
                options.CollectionPrefix = "orders";
            });

        services.AddMongoProjectionsDataStore<SecondaryTestModel>(
            InvoicesSectionName,
            "mongodb://localhost:27017",
            "testdb",
            options =>
            {
                options.CollectionPrefix = "invoices";
            });

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IModelStateStore<TestModel>>(GetProjectionKey<TestModel>(OrdersSectionName)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IModelStateStore<SecondaryTestModel>>(GetProjectionKey<SecondaryTestModel>(InvoicesSectionName)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IProjectionFailureHandler<TestModel>>(GetProjectionKey<TestModel>(OrdersSectionName)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IProjectionFailureHandler<SecondaryTestModel>>(GetProjectionKey<SecondaryTestModel>(InvoicesSectionName))
                .ShouldNotBeNull();

        provider.GetRequiredKeyedService<ICheckpointStore>(GetProjectionKey<TestModel>(OrdersSectionName)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<ICheckpointStore>(GetProjectionKey<SecondaryTestModel>(InvoicesSectionName)).ShouldNotBeNull();

        provider.GetServices<IProjectionStartupTask>().Count().ShouldBe(2);
    }

    [Fact]
    public void AddMongoProjectionsDataStore_Should_Use_Custom_Collection_Namer_Factory()
    {
        ServiceCollection services = new();
        var factoryCalled = false;

        services.AddMongoProjectionsDataStore<TestModel>(
            OrdersSectionName,
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

    private static string GetProjectionKey<TState>(string settingsSectionName)
        where TState : class, Kuna.Projections.Abstractions.Models.IModel, new()
    {
        return ProjectionRegistration.GetKey<TState>(settingsSectionName);
    }
}
