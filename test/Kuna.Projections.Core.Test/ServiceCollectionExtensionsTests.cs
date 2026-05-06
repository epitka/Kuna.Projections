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
        services.AddLogging();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(new Dictionary<string, string?>())
                            .Build();

        var exception = Should.Throw<InvalidOperationException>(
            () =>
                services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name));

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
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:ModelCountThreshold"] = "12",
                                    [$"{ProjectionSettingsSection.Name}:LiveProcessingFlush:ModelCountThreshold"] = "7",
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredKeyedService<IProjectionSettings<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name));

        settings.CatchUpFlush.ModelCountThreshold.ShouldBe(12);
        settings.LiveProcessingFlush.ModelCountThreshold.ShouldBe(7);
        settings.Backpressure.SourceToTransformBufferCapacity.ShouldBe(10000);
        settings.Backpressure.TransformToSinkBufferCapacity.ShouldBe(10000);
        settings.LiveProcessingFlush.Delay.ShouldBe(1000);
        settings.CatchUpFlush.Strategy.ShouldBe(PersistenceStrategy.ModelCountBatching);
        settings.LiveProcessingFlush.Strategy.ShouldBe(PersistenceStrategy.ImmediateModelFlush);
        settings.ModelStateCacheCapacity.ShouldBe(10000);
        settings.EventVersionCheckStrategy.ShouldBe(EventVersionCheckStrategy.Consecutive);
        settings.InstanceId.ShouldBe("orders-v1");
    }

    [Fact]
    public void AddProjectionCore_Should_Register_Core_Services_When_Config_Section_Is_Present()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateTransformer<EventEnvelope, CoreServiceTestModel>)
                && Equals(sd.ServiceKey, GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name))
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionLifecycle<CoreServiceTestModel>)
                && Equals(sd.ServiceKey, GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name))
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IModelStateCache<CoreServiceTestModel>)
                && Equals(sd.ServiceKey, GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name))
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionPipeline<CoreServiceTestModel>)
                && Equals(sd.ServiceKey, GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name))
                && sd.Lifetime == ServiceLifetime.Singleton);

        services.ShouldContain(
            sd =>
                sd.ServiceType == typeof(IProjectionFactory<CoreServiceTestModel>)
                && Equals(sd.ServiceKey, GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name))
                && sd.Lifetime == ServiceLifetime.Singleton);
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
                                    ["OrdersProjection:InstanceId"] = "orders-v2",
                                    ["OrdersProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(
            configuration,
            settingsSectionName: "OrdersProjection");

        using var provider = services.BuildServiceProvider();
        var settings = provider.GetRequiredKeyedService<IProjectionSettings<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection"));

        settings.CatchUpFlush.Strategy.ShouldBe(PersistenceStrategy.ModelCountBatching);
        settings.LiveProcessingFlush.Strategy.ShouldBe(PersistenceStrategy.ImmediateModelFlush);
        settings.CatchUpFlush.ModelCountThreshold.ShouldBe(100);
        settings.LiveProcessingFlush.ModelCountThreshold.ShouldBe(100);
        settings.Backpressure.SourceToTransformBufferCapacity.ShouldBe(10000);
        settings.Backpressure.TransformToSinkBufferCapacity.ShouldBe(10000);
        settings.LiveProcessingFlush.Delay.ShouldBe(1000);
        settings.ModelStateCacheCapacity.ShouldBe(10000);
        settings.EventVersionCheckStrategy.ShouldBe(EventVersionCheckStrategy.Consecutive);
        settings.InstanceId.ShouldBe("orders-v2");
    }

    [Fact]
    public void AddProjectionCore_Should_Throw_When_InstanceId_Is_Missing()
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

        var exception = Should.Throw<InvalidOperationException>(() => services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name));

        exception.Message.ShouldContain(nameof(IProjectionSettings<CoreServiceTestModel>.InstanceId));
        exception.Message.ShouldContain(ProjectionSettingsSection.Name);
    }

    [Fact]
    public void AddProjectionCore_Should_Throw_When_Projection_Does_Not_Have_Guid_Ctor()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        AddStateStore<BadCtorModel, BadCtorStateStore>(services, ProjectionSettingsSection.Name);
        services.AddProjection<BadCtorModel>(configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () =>
                provider.GetRequiredKeyedService<IProjectionFactory<BadCtorModel>>(GetProjectionKey<BadCtorModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain(typeof(BadCtorProjection).FullName!);
        ex.Message.ShouldContain("Guid parameter");
    }

    [Fact]
    public void AddProjectionCore_Should_Require_Initial_Event_Registration_For_Runtime_Resolution()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        AddStateStore<CoreServiceTestModel, DummyStateStore>(services, ProjectionSettingsSection.Name);
        AddFailureHandler<CoreServiceTestModel, DummyFailureHandler>(services, ProjectionSettingsSection.Name);
        services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();
        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredKeyedService<ProjectionEngine<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain(nameof(ProjectionCreationRegistration<CoreServiceTestModel>));
    }

    [Fact]
    public void AddProjectionCore_Should_Allow_Registering_Initial_Event()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name)
                .WithInitialEvent<TestInitialEvent>();

        using var provider = services.BuildServiceProvider();
        var registration = GetProjectionCreationRegistration<CoreServiceTestModel>(provider, ProjectionSettingsSection.Name);

        GetInitialEventType(registration).ShouldBe(typeof(TestInitialEvent));
    }

    [Fact]
    public void AddProjectionCore_Should_Throw_When_Initial_Event_Is_Registered_Twice()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
                            .AddInMemoryCollection(
                                new Dictionary<string, string?>
                                {
                                    [$"{ProjectionSettingsSection.Name}:InstanceId"] = "orders-v1",
                                    [$"{ProjectionSettingsSection.Name}:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        var builder = services.AddProjection<CoreServiceTestModel>(configuration, ProjectionSettingsSection.Name)
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
                                    ["OrdersProjection:InstanceId"] = "orders-v1",
                                    ["OrdersProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                    ["InvoicesProjection:InstanceId"] = "invoices-v1",
                                    ["InvoicesProjection:CatchUpFlush:Strategy"] = PersistenceStrategy.ModelCountBatching.ToString(),
                                })
                            .Build();

        AddStateStore<CoreServiceTestModel, DummyStateStore>(services, "OrdersProjection");
        AddStateSink<CoreServiceTestModel, DummyStateSink>(services, "OrdersProjection");
        AddFailureHandler<CoreServiceTestModel, DummyFailureHandler>(services, "OrdersProjection");
        services.AddKeyedSingleton<IProjectionEventSource<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection"), new TestProjectionEventSource<CoreServiceTestModel>());
        var checkpointStore = new DummyCheckpointStore();
        services.AddKeyedSingleton<ICheckpointStore>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection"), checkpointStore);

        AddStateStore<SecondaryServiceTestModel, SecondaryStateStore>(services, "InvoicesProjection");
        AddStateSink<SecondaryServiceTestModel, SecondaryStateSink>(services, "InvoicesProjection");
        AddFailureHandler<SecondaryServiceTestModel, SecondaryFailureHandler>(services, "InvoicesProjection");
        services.AddKeyedSingleton<IProjectionEventSource<SecondaryServiceTestModel>>(GetProjectionKey<SecondaryServiceTestModel>("InvoicesProjection"), new TestProjectionEventSource<SecondaryServiceTestModel>());
        services.AddKeyedSingleton<ICheckpointStore>(GetProjectionKey<SecondaryServiceTestModel>("InvoicesProjection"), checkpointStore);

        services.AddProjection<CoreServiceTestModel>(configuration, settingsSectionName: "OrdersProjection")
                .WithInitialEvent<TestInitialEvent>();

        services.AddProjection<SecondaryServiceTestModel>(configuration, settingsSectionName: "InvoicesProjection")
                .WithInitialEvent<SecondaryInitialEvent>();

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IProjectionPipeline>().Count().ShouldBe(2);

        var ordersPipeline = provider.GetRequiredKeyedService<IProjectionPipeline<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection"));
        var invoicesPipeline = provider.GetRequiredKeyedService<IProjectionPipeline<SecondaryServiceTestModel>>(GetProjectionKey<SecondaryServiceTestModel>("InvoicesProjection"));

        GetPrivateField<object>(ordersPipeline, "source").ShouldBeSameAs(provider.GetRequiredKeyedService<IProjectionEventSource<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection")).Value);
        GetPrivateField<object>(ordersPipeline, "checkpointStore")
            .ShouldBeSameAs(checkpointStore);

        GetPrivateField<object>(ordersPipeline, "lifecycle").ShouldBeSameAs(provider.GetRequiredKeyedService<IProjectionLifecycle<CoreServiceTestModel>>(GetProjectionKey<CoreServiceTestModel>("OrdersProjection")));

        GetPrivateField<object>(invoicesPipeline, "source")
            .ShouldBeSameAs(provider.GetRequiredKeyedService<IProjectionEventSource<SecondaryServiceTestModel>>(GetProjectionKey<SecondaryServiceTestModel>("InvoicesProjection")).Value);

        GetPrivateField<object>(invoicesPipeline, "checkpointStore")
            .ShouldBeSameAs(checkpointStore);

        GetPrivateField<object>(invoicesPipeline, "lifecycle").ShouldBeSameAs(provider.GetRequiredKeyedService<IProjectionLifecycle<SecondaryServiceTestModel>>(GetProjectionKey<SecondaryServiceTestModel>("InvoicesProjection")));
    }

    private static object GetProjectionCreationRegistration<TState>(IServiceProvider provider, string settingsSectionName)
        where TState : class, IModel, new()
    {
        var registrationType = typeof(ServiceCollectionExtensions).Assembly
                                                                  .GetType("Kuna.Projections.Core.ProjectionCreationRegistration`1")!
                                                                  .MakeGenericType(typeof(TState));

        return provider.GetRequiredKeyedService(registrationType, GetProjectionKey<TState>(settingsSectionName));
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

    private static string GetProjectionKey<TState>(string settingsSectionName)
        where TState : class, IModel, new()
    {
        return ProjectionRegistration.GetKey<TState>(settingsSectionName);
    }

    private static void AddStateStore<TState, TStore>(IServiceCollection services, string settingsSectionName)
        where TState : class, IModel, new()
        where TStore : class, IModelStateStore<TState>
    {
        services.AddKeyedSingleton<IModelStateStore<TState>, TStore>(GetProjectionKey<TState>(settingsSectionName));
    }

    private static void AddStateSink<TState, TSink>(IServiceCollection services, string settingsSectionName)
        where TState : class, IModel, new()
        where TSink : class, IModelStateSink<TState>
    {
        services.AddKeyedSingleton<IModelStateSink<TState>, TSink>(GetProjectionKey<TState>(settingsSectionName));
    }

    private static void AddFailureHandler<TState, THandler>(IServiceCollection services, string settingsSectionName)
        where TState : class, IModel, new()
        where THandler : class, IProjectionFailureHandler<TState>
    {
        services.AddKeyedSingleton<IProjectionFailureHandler<TState>, THandler>(GetProjectionKey<TState>(settingsSectionName));
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
        public Task<CheckPoint> GetCheckpoint(string modelName, string instanceId, CancellationToken cancellationToken)
        {
            return Task.FromResult(
                new CheckPoint
                {
                    ModelName = modelName,
                    InstanceId = instanceId,
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
