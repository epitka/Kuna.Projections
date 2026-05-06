using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Kuna.Projections.Sink.EF.MySql;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

public class MySqlServiceCollectionExtensionsTests
{
    [Fact]
    public void AddMySqlProjectionsDataStore_Should_Register_Provider_Detector_And_Common_Services()
    {
        var services = new ServiceCollection();
        var registrationKey = ProjectionRegistration.GetKey<TestModel>("OrdersProjection");

        var returned = services.AddMySqlProjectionsDataStore<TestModel, TestProjectionDbContext>("OrdersProjection", schema: "orders");

        returned.ShouldBeSameAs(services);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IDuplicateKeyExceptionDetector)
                && sd.ImplementationType != null
                && sd.ImplementationType.FullName == "Kuna.Projections.Sink.EF.MySql.MySqlDuplicateKeyExceptionDetector"
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionFailureHandler<TestModel>)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateSink<TestModel>)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateStore<TestModel>)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(ICheckpointStore)
                && Equals(sd.ServiceKey, registrationKey)
                && sd.Lifetime == ServiceLifetime.Singleton);
    }
}
