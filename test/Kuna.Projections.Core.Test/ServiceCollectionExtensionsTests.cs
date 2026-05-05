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
        services.AddSingleton<IProjectionFailureHandler<CoreServiceTestModel>, DummyFailureHandler>();
        services.AddLogging();

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
        settings.ModelStateCacheCapacity.ShouldBe(10000);
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
                sd.ServiceType == typeof(IProjectionLifecycle<CoreServiceTestModel>)
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
        settings.ModelStateCacheCapacity.ShouldBe(10000);
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

    [Fact]
    public void AddProjectionCore_Should_Require_Initial_Event_Registration_For_Runtime_Resolution()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();
        services.AddSingleton<IProjectionFailureHandler<CoreServiceTestModel>, DummyFailureHandler>();
        services.AddLogging();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration);

        using var provider = services.BuildServiceProvider();
        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<ProjectionEngine<CoreServiceTestModel>>());

        ex.Message.ShouldContain(nameof(ProjectionCreationRegistration<CoreServiceTestModel>));
    }

    [Fact]
    public void AddProjectionCore_Should_Allow_Registering_Initial_Event()
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

        services.AddProjection<CoreServiceTestModel>(configuration)
                .WithInitialEvent<TestInitialEvent>();

        using var provider = services.BuildServiceProvider();
        var registration = GetProjectionCreationRegistration<CoreServiceTestModel>(provider);

        GetInitialEventType(registration).ShouldBe(typeof(TestInitialEvent));
    }

    [Fact]
    public void AddProjectionCore_Should_Throw_When_Initial_Event_Is_Registered_Twice()
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

        var builder = services.AddProjection<CoreServiceTestModel>(configuration)
                              .WithInitialEvent<TestInitialEvent>();

        var ex = Should.Throw<InvalidOperationException>(() => builder.WithInitialEvent<AnotherInitialEvent>());

        ex.Message.ShouldContain(typeof(CoreServiceTestModel).FullName!);
        ex.Message.ShouldContain(typeof(TestInitialEvent).FullName!);
    }

    [Fact]
    public void AddProjectionCore_Should_Isolate_Typed_Source_Lifecycle_And_Checkpoint_Dependencies()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    ["OrdersProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                    ["InvoicesProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddSingleton<IModelStateStore<CoreServiceTestModel>, DummyStateStore>();
        services.AddSingleton<IModelStateSink<CoreServiceTestModel>, DummyStateSink>();
        services.AddSingleton<IProjectionFailureHandler<CoreServiceTestModel>, DummyFailureHandler>();
        services.AddSingleton<IProjectionEventSource<CoreServiceTestModel>>(new TestProjectionEventSource<CoreServiceTestModel>());
        var checkpointStore = new DummyCheckpointStore();
        services.AddSingleton<ICheckpointStore>(checkpointStore);

        services.AddSingleton<IModelStateStore<SecondaryServiceTestModel>, SecondaryStateStore>();
        services.AddSingleton<IModelStateSink<SecondaryServiceTestModel>, SecondaryStateSink>();
        services.AddSingleton<IProjectionFailureHandler<SecondaryServiceTestModel>, SecondaryFailureHandler>();
        services.AddSingleton<IProjectionEventSource<SecondaryServiceTestModel>>(new TestProjectionEventSource<SecondaryServiceTestModel>());

        services.AddProjection<CoreServiceTestModel>(configuration, settingsSectionName: "OrdersProjection")
                .WithInitialEvent<TestInitialEvent>();

        services.AddProjection<SecondaryServiceTestModel>(configuration, settingsSectionName: "InvoicesProjection")
                .WithInitialEvent<SecondaryInitialEvent>();

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IProjectionPipeline>().Count().ShouldBe(2);

        var ordersPipeline = provider.GetRequiredService<IProjectionPipeline<CoreServiceTestModel>>();
        var invoicesPipeline = provider.GetRequiredService<IProjectionPipeline<SecondaryServiceTestModel>>();

        GetPrivateField<object>(ordersPipeline, "source").ShouldBeSameAs(provider.GetRequiredService<IProjectionEventSource<CoreServiceTestModel>>().Value);
        GetPrivateField<object>(ordersPipeline, "checkpointStore")
            .ShouldBeSameAs(checkpointStore);

        GetPrivateField<object>(ordersPipeline, "lifecycle").ShouldBeSameAs(provider.GetRequiredService<IProjectionLifecycle<CoreServiceTestModel>>());

        GetPrivateField<object>(invoicesPipeline, "source")
            .ShouldBeSameAs(provider.GetRequiredService<IProjectionEventSource<SecondaryServiceTestModel>>().Value);

        GetPrivateField<object>(invoicesPipeline, "checkpointStore")
            .ShouldBeSameAs(checkpointStore);

        GetPrivateField<object>(invoicesPipeline, "lifecycle").ShouldBeSameAs(provider.GetRequiredService<IProjectionLifecycle<SecondaryServiceTestModel>>());
    }

    private static object GetProjectionCreationRegistration<TState>(IServiceProvider provider)
        where TState : class, IModel, new()
    {
        var registrationType = typeof(ServiceCollectionExtensions).Assembly
                                                                  .GetType("Kuna.Projections.Core.ProjectionCreationRegistration`1")!
                                                                  .MakeGenericType(typeof(TState));

        return provider.GetRequiredService(registrationType);
    }

    private static Type GetInitialEventType(object registration)
    {
        return (Type)registration.GetType().GetProperty("InitialEventType")!.GetValue(registration)!;
    }

    private static TField GetPrivateField<TField>(object target, string fieldName)
    {
        return (TField)target.GetType()
                             .GetField(fieldName, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
                             .GetValue(target)!;
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

    public sealed class TestInitialEvent : Event
    {
    }

    public sealed class AnotherInitialEvent : Event
    {
    }

    public sealed class SecondaryInitialEvent : Event
    {
    }

    public sealed class BadCtorModel : Model
    {
    }

    public sealed class SecondaryServiceTestModel : Model
    {
    }

    public sealed class BadCtorProjection : Projection<BadCtorModel>
    {
        public BadCtorProjection(string id)
            : base(Guid.Empty)
        {
        }
    }

    public sealed class SecondaryServiceTestProjection : Projection<SecondaryServiceTestModel>
    {
        public SecondaryServiceTestProjection(Guid id)
            : base(id)
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

    private sealed class DummyStateSink : IModelStateSink<CoreServiceTestModel>
    {
        public Task PersistBatch(ModelStatesBatch<CoreServiceTestModel> batch, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class DummyFailureHandler : IProjectionFailureHandler<CoreServiceTestModel>
    {
        public Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class SecondaryStateStore : IModelStateStore<SecondaryServiceTestModel>
    {
        public Task<SecondaryServiceTestModel?> Load(Guid modelId, CancellationToken cancellationToken)
        {
            return Task.FromResult<SecondaryServiceTestModel?>(null);
        }
    }

    private sealed class SecondaryStateSink : IModelStateSink<SecondaryServiceTestModel>
    {
        public Task PersistBatch(ModelStatesBatch<SecondaryServiceTestModel> batch, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class SecondaryFailureHandler : IProjectionFailureHandler<SecondaryServiceTestModel>
    {
        public Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestProjectionEventSource<TState> : IProjectionEventSource<TState>
        where TState : class, IModel, new()
    {
        public TestProjectionEventSource()
        {
            this.Value = new DummyEventSource();
        }

        public IEventSource<EventEnvelope> Value { get; }
    }

    private sealed class DummyEventSource : IEventSource<EventEnvelope>
    {
        public async IAsyncEnumerable<EventEnvelope> ReadAll(
            GlobalEventPosition start,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class DummyCheckpointStore : ICheckpointStore
    {
        public Task<CheckPoint> GetCheckpoint(string modelName, CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new CheckPoint
                {
                    ModelName = modelName,
                    GlobalEventPosition = new GlobalEventPosition(string.Empty),
                });
        }

        public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
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
