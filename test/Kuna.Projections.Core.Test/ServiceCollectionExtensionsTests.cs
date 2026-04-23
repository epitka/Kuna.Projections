using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddProjectionCore_Should_Throw_When_Projections_Config_Section_Is_Missing()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(new Dictionary<string, string?>())
                            .Build();

        var exception = Should.Throw<InvalidOperationException>(
            () =>
                services.AddProjection<CoreServiceTestModel>(configuration));

        exception.Message.ShouldContain(ProjectionSettingsSection.Name);
    }

    [Fact]
    public void AddProjectionCore_Should_Use_Defaults_When_Optional_Settings_Are_Missing()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:ModelCountThreshold"] = "12",
                                    [$"{ProjectionSettingsSection.Name}:LiveProcessingFlush:ModelCountThreshold"] = "7",
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration);

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IProjectionSettings<CoreServiceTestModel>>();

        settings.CatchUpFlush.ModelCountThreshold.ShouldBe(12);
        settings.LiveProcessingFlush.ModelCountThreshold.ShouldBe(7);
        settings.Backpressure.SourceToTransformBufferCapacity.ShouldBe(10000);
        settings.Backpressure.TransformToSinkBufferCapacity.ShouldBe(10000);
        settings.LiveProcessingFlush.Delay.ShouldBe(1000);
        settings.CatchUpFlush.Strategy.ShouldBe(PersistenceStrategy.ModelCountBatching);
        settings.LiveProcessingFlush.Strategy.ShouldBe(PersistenceStrategy.ImmediateModelFlush);
        settings.SkipStateNotFoundFailure.ShouldBeFalse();
        settings.InFlightModelCacheMinEntries.ShouldBe(10000);
        settings.InFlightModelCacheCapacityMultiplier.ShouldBe(3);
        settings.EventVersionCheckStrategy.ShouldBe(EventVersionCheckStrategy.Consecutive);
    }

    [Fact]
    public void AddProjectionCore_Should_Register_Core_Services_When_Config_Section_Is_Present()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration);

        using var provider = services.BuildServiceProvider();
        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateTransformer<EventEnvelope, CoreServiceTestModel>)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionLifecycle)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateCache<CoreServiceTestModel>)
                && sd.ImplementationType == typeof(InMemoryModelStateCache<CoreServiceTestModel>)
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionPipeline<CoreServiceTestModel>)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionFactory<CoreServiceTestModel>)
                && sd.ImplementationFactory != null
                && sd.Lifetime == ServiceLifetime.Singleton);

        provider.GetRequiredService<IProjectionFactory<CoreServiceTestModel>>().ShouldNotBeNull();
    }

    [Fact]
    public void AddProjectionCore_Should_Bind_Settings_From_Custom_Section_When_Provided()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    ["OrdersProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(
            configuration,
            settingsSectionName: "OrdersProjection");

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredService<IProjectionSettings<CoreServiceTestModel>>();

        settings.CatchUpFlush.Strategy.ShouldBe(PersistenceStrategy.ModelCountBatching);
        settings.LiveProcessingFlush.Strategy.ShouldBe(PersistenceStrategy.ImmediateModelFlush);
        settings.CatchUpFlush.ModelCountThreshold.ShouldBe(100);
        settings.LiveProcessingFlush.ModelCountThreshold.ShouldBe(100);
        settings.Backpressure.SourceToTransformBufferCapacity.ShouldBe(10000);
        settings.Backpressure.TransformToSinkBufferCapacity.ShouldBe(10000);
        settings.LiveProcessingFlush.Delay.ShouldBe(1000);
        settings.SkipStateNotFoundFailure.ShouldBeFalse();
        settings.InFlightModelCacheMinEntries.ShouldBe(10000);
        settings.InFlightModelCacheCapacityMultiplier.ShouldBe(3);
        settings.EventVersionCheckStrategy.ShouldBe(EventVersionCheckStrategy.Consecutive);
    }

    [Fact]
    public void AddProjectionCore_Should_Throw_When_Projection_Does_Not_Have_Guid_Ctor()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<BadCtorModel>, BadCtorStateStore>();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<BadCtorModel>(configuration);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () =>
                provider.GetRequiredService<IProjectionFactory<BadCtorModel>>());

        ex.Message.ShouldContain(typeof(BadCtorProjection).FullName!);
        ex.Message.ShouldContain("Guid parameter");
    }

    public sealed class CoreServiceTestModel : Model
    {
    }

    public sealed class CoreServiceTestProjection : Projection<CoreServiceTestModel>
    {
        public CoreServiceTestProjection(Guid id)
            : base(id)
        {
        }
    }

    public sealed class BadCtorModel : Model
    {
    }

    public sealed class BadCtorProjection : Projection<BadCtorModel>
    {
        public BadCtorProjection(string id)
            : base(Guid.Empty)
        {
        }
    }

    private sealed class DummyStateStore : IModelStateStore<CoreServiceTestModel>
    {
        public Task<CoreServiceTestModel?> Load(Guid modelId, CancellationToken cancellationToken)
        {
            return Task.FromResult<CoreServiceTestModel?>(null);
        }
    }

    private sealed class BadCtorStateStore : IModelStateStore<BadCtorModel>
    {
        public Task<BadCtorModel?> Load(Guid modelId, CancellationToken cancellationToken)
        {
            return Task.FromResult<BadCtorModel?>(null);
        }
    }
}
