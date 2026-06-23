using FakeItEasy;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Events;
using Kuna.Projections.Core.Test.Shared.Models;
using Kuna.Projections.Core.Test.Shared.Projections;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionEngineTests;

public class TransformTests
{
    [Fact]
    public async Task When_State_Not_Found_Should_Persist_Failure_And_Skip_Subsequent_Events()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>();
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns((Projection<ItemModel>?)null);

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        var first = await transformer.Transform(CreateEnvelope(modelId, 1, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);
        var second = await transformer.Transform(CreateEnvelope(modelId, 2, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        first.ShouldBeNull();
        second.ShouldBeNull();

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(
             () => handler.Handle(
                 A<ProjectionFailure>.That.Matches(
                     f =>
                         f.ModelId == modelId && f.EventNumber == 1 && f.FailureType == FailureType.EventProcessing.ToString()),
                 A<CancellationToken>._))
         .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task When_Event_Out_Of_Order_Should_Persist_Failure_And_Throw()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>();
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        var projection = new ItemProjection(modelId)
        {
            IsNew = false,
        };

        projection.ModelState.EventNumber = 0;
        projection.ModelState.GlobalEventPosition = new GlobalEventPosition("0");

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns(projection);

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        await Should.ThrowAsync<Kuna.Projections.Abstractions.Exceptions.EventOutOfOrderException>(
            () =>
                transformer.Transform(
                               CreateEnvelope(modelId, 2, new ItemUpdated { Id = modelId, Name = "x", TypeName = nameof(ItemUpdated), }),
                               CancellationToken.None)
                           .AsTask());

        A.CallTo(
             () => handler.Handle(
                 A<ProjectionFailure>.That.Matches(
                     f =>
                         f.ModelId == modelId && f.EventNumber == 2 && f.FailureType == FailureType.EventOutOfOrder.ToString()),
                 A<CancellationToken>._))
         .MustHaveHappenedOnceExactly();

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task When_Projection_Throws_Unexpected_Exception_Should_Rethrow()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._))
         .Returns(new ItemProjection(modelId));

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<TestEvent>(),
            settings,
            logger);

        await Should.ThrowAsync<Exception>(
            () =>
                transformer.Transform(CreateEnvelope(modelId, 0, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Should_Not_Load_State_From_Store_For_Initial_Event()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._))
         .Returns(new ItemProjection(modelId));

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        var result = await transformer.Transform(
                         CreateEnvelope(modelId, 0, new ItemCreated { Id = modelId, Name = "n1", TypeName = nameof(ItemCreated), }),
                         CancellationToken.None);

        result.ShouldNotBeNull();

        // A model created from its initial event is a brand-new aggregate with no
        // prior version, so its expected (pre-event) version is the "no prior
        // version" sentinel (-1).
        result!.IsNew.ShouldBeTrue();
        result.ExpectedEventNumber.ShouldBe(-1);
        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Initial_Event_With_Non_Zero_Global_Position_Should_Stay_An_Insert()
    {
        // Regression: EventSourcingDB delivers a single, globally ordered stream and
        // maps the global event id onto EventNumber. Every aggregate's initial event
        // therefore carries a different, mostly non-zero EventNumber. Such a model
        // must still be classified as a new aggregate (ExpectedEventNumber < 0) so
        // the sink inserts it; deriving the expected version as eventNumber - 1 made
        // every aggregate except the globally first one look like an update.
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings(EventVersionCheckStrategy.Monotonic);
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var firstModelId = Guid.NewGuid();
        var secondModelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(firstModelId, false, A<CancellationToken>._))
         .Returns(new ItemProjection(firstModelId));

        A.CallTo(() => factory.Create(secondModelId, false, A<CancellationToken>._))
         .Returns(new ItemProjection(secondModelId));

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        var firstGlobal = await transformer.Transform(
                              CreateEnvelope(firstModelId, 0, new ItemCreated { Id = firstModelId, Name = "first", TypeName = nameof(ItemCreated), }),
                              CancellationToken.None);

        var secondGlobal = await transformer.Transform(
                               CreateEnvelope(secondModelId, 4711, new ItemCreated { Id = secondModelId, Name = "second", TypeName = nameof(ItemCreated), }),
                               CancellationToken.None);

        firstGlobal.ShouldNotBeNull();
        secondGlobal.ShouldNotBeNull();

        firstGlobal!.IsNew.ShouldBeTrue();
        secondGlobal!.IsNew.ShouldBeTrue();

        // Neither carries a prior version, so the pipeline keeps both as inserts.
        firstGlobal.ExpectedEventNumber.ShouldBe(-1);
        secondGlobal.ExpectedEventNumber.ShouldBe(-1);
    }

    [Fact]
    public async Task Should_Load_State_From_Store_For_Non_Initial_Event()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        var projection = new ItemProjection(modelId)
        {
            IsNew = false,
        };

        projection.ModelState.EventNumber = 0;
        projection.ModelState.Name = "old";

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns(projection);

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        var result = await transformer.Transform(
                         CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "new", TypeName = nameof(ItemUpdated), }),
                         CancellationToken.None);

        result.ShouldNotBeNull();
        result!.Model.Name.ShouldBe("new");
        result.IsNew.ShouldBeFalse();
        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Should_Reuse_Cached_Projection_And_Create_Again_After_Clear()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>();
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();
        var createCalls = 0;

        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._))
         .ReturnsLazily(
             _ =>
             {
                 createCalls++;
                 return new ValueTask<Projection<ItemModel>?>(new ItemProjection(modelId));
             });

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .ReturnsLazily(
             _ =>
             {
                 createCalls++;
                 var p = new ItemProjection(modelId) { IsNew = false, };
                 p.ModelState.EventNumber = 1;
                 p.ModelState.Name = "restored";
                 return new ValueTask<Projection<ItemModel>?>(p);
             });

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        var first = await transformer.Transform(
                        CreateEnvelope(modelId, 0, new ItemCreated { Id = modelId, Name = "a", TypeName = nameof(ItemCreated), }),
                        CancellationToken.None);

        var second = await transformer.Transform(
                         CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "b", TypeName = nameof(ItemUpdated), }),
                         CancellationToken.None);

        createCalls.ShouldBe(1);
        first.ShouldNotBeNull();
        second.ShouldNotBeNull();

        transformer.OnFlushSucceeded([modelId,], [modelId,], new Dictionary<Guid, long?> { [modelId] = 1, });

        var third = await transformer.Transform(
                        CreateEnvelope(modelId, 2, new ItemUpdated { Id = modelId, Name = "c", TypeName = nameof(ItemUpdated), }),
                        CancellationToken.None);

        third.ShouldNotBeNull();
        createCalls.ShouldBe(2);
    }

    [Fact]
    public async Task Should_Recreate_From_Model_State_Cache_As_Update_After_Flush()
    {
        var innerFactory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict()));

        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var factory = new RecordingProjectionFactory(innerFactory);

        cache.Set(
            new ModelState<ItemModel>(
                new ItemModel
                {
                    Id = modelId,
                    EventNumber = 0,
                    GlobalEventPosition = new GlobalEventPosition("10"),
                    Name = "created",
                },
                IsNew: true,
                ShouldDelete: false,
                GlobalEventPosition: new GlobalEventPosition("10"),
                ExpectedEventNumber: null));

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, cache, CreateRegistration<ItemCreated>(), settings, logger);

        var result = await transformer.Transform(
                         CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "updated", TypeName = nameof(ItemUpdated), }),
                         CancellationToken.None);

        result.ShouldNotBeNull();
        result.IsNew.ShouldBeFalse();
        result.ExpectedEventNumber.ShouldBe(0);
        result.Model.Name.ShouldBe("updated");
        factory.CreateFromModelIsNewValues.ShouldBe([false,]);
    }

    [Fact]
    public async Task ClearAll_Should_Reset_Failed_Projection_Tracking()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>();
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>();
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns((Projection<ItemModel>?)null);

        var transformer = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            new InMemoryModelStateCache<ItemModel>(settings),
            CreateRegistration<ItemCreated>(),
            settings,
            logger);

        await transformer.Transform(CreateEnvelope(modelId, 1, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);
        await transformer.Transform(CreateEnvelope(modelId, 2, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();

        transformer.ClearAll();

        await transformer.Transform(CreateEnvelope(modelId, 3, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedTwiceExactly();
    }

    private static ProjectionSettings<ItemModel> CreateSettings(EventVersionCheckStrategy versionCheckStrategy = EventVersionCheckStrategy.Consecutive)
    {
        return new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 100,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
            EventVersionCheckStrategy = versionCheckStrategy,
        };
    }

    private static EventEnvelope CreateEnvelope(Guid modelId, long eventNumber, Event @event)
    {
        @event.TypeName = string.IsNullOrWhiteSpace(@event.TypeName) ? @event.GetType().Name : @event.TypeName;
        @event.CreatedOn = @event.CreatedOn == default ? DateTime.UtcNow : @event.CreatedOn;

        return new EventEnvelope(
            eventNumber: eventNumber,
            streamPosition: new GlobalEventPosition((eventNumber + 10).ToString()),
            streamId: $"item-{modelId}",
            modelId: modelId,
            createdOn: DateTime.UtcNow,
            @event: @event);
    }

    private static ProjectionCreationRegistration<ItemModel> CreateRegistration<TEvent>()
        where TEvent : Event
    {
        return new ProjectionCreationRegistration<ItemModel>(typeof(TEvent));
    }

    private sealed class RecordingProjectionFactory : IProjectionFactory<ItemModel>
    {
        private readonly IProjectionFactory<ItemModel> inner;

        public RecordingProjectionFactory(IProjectionFactory<ItemModel> inner)
        {
            this.inner = inner;
        }

        public List<bool> CreateFromModelIsNewValues { get; } = [];

        public ValueTask<Projection<ItemModel>?> Create(Guid modelId, bool loadModelFromStore, CancellationToken cancellationToken)
        {
            return this.inner.Create(modelId, loadModelFromStore, cancellationToken);
        }

        public Projection<ItemModel> CreateFromModel(ItemModel model, bool isNew)
        {
            this.CreateFromModelIsNewValues.Add(isNew);
            return this.inner.CreateFromModel(model, isNew);
        }
    }
}
