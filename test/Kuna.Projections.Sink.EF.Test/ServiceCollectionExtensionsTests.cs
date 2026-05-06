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
        var returned = services.AddSqlProjectionsDataStore<TestModel, TestProjectionDbContext>("OrdersProjection", schema: "dbo");
        var registrationKey = ProjectionRegistration.GetKey<TestModel>("OrdersProjection");

        returned.ShouldBeSameAs(services);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionFailureHandler<TestModel>)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(DataStore<TestModel, TestProjectionDbContext>)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateSink<TestModel>) && Equals(sd.ServiceKey, registrationKey) && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateStore<TestModel>) && Equals(sd.ServiceKey, registrationKey) && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(ICheckpointStore)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);
    }
}
