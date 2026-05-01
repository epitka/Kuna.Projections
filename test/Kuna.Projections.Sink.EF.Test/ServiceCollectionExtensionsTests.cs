using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddEfProjectionStore_Should_Register_Required_Services()
    {
        var services = new ServiceCollection();
        var returned = services.AddSqlProjectionsDataStore<TestModel, TestProjectionDbContext>(schema: "dbo");

        returned.ShouldBeSameAs(services);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionFailureHandler<TestModel>)
                && sd.ImplementationType == typeof(ProjectionFailureHandler<TestModel, TestProjectionDbContext>)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(DataStore<TestModel, TestProjectionDbContext>)
                && sd.ImplementationType == typeof(DataStore<TestModel, TestProjectionDbContext>)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateSink<TestModel>) && sd.ImplementationFactory != null && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateStore<TestModel>) && sd.ImplementationFactory != null && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(ICheckpointStore)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);
    }
}
