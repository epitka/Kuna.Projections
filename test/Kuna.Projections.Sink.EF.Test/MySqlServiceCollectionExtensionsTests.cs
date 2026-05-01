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

        var returned = services.AddMySqlProjectionsDataStore<TestModel, TestProjectionDbContext>(schema: "orders");

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
                && sd.ImplementationType == typeof(ProjectionFailureHandler<TestModel, TestProjectionDbContext>)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateSink<TestModel>)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateStore<TestModel>)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(ICheckpointStore)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);
    }
}
