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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

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
        projection.ModelState.GlobalEventPosition = new GlobalEventPosition(0);

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns(projection);

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

        await Should.ThrowAsync<Exception>(
            () =>
                transformer.Transform(CreateEnvelope(modelId, 0, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Should_Not_Load_State_From_Store_For_First_Event()
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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

        var result = await transformer.Transform(
                         CreateEnvelope(modelId, 0, new ItemCreated { Id = modelId, Name = "n1", TypeName = nameof(ItemCreated), }),
                         CancellationToken.None);

        result.ShouldNotBeNull();
        A.CallTo(() => factory.Create(modelId, false, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Should_Load_State_From_Store_For_Non_First_Event()
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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

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
        var factory = A.Fake<IProjectionFactory<ItemModel>>(opt => opt.Strict());
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = CreateSettings();
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var createFromModelIsNew = new List<bool>();

        cache.Set(
            new ModelState<ItemModel>(
                new ItemModel
                {
                    Id = modelId,
                    EventNumber = 0,
                    GlobalEventPosition = new GlobalEventPosition(10),
                    Name = "created",
                },
                IsNew: true,
                ShouldDelete: false,
                GlobalEventPosition: new GlobalEventPosition(10),
                ExpectedEventNumber: null));

        A.CallTo(() => factory.CreateFromModel(A<ItemModel>._, A<bool>._))
         .ReturnsLazily(
             (ItemModel model, bool isNew) =>
             {
                 createFromModelIsNew.Add(isNew);
                 var projection = new ItemProjection(model.Id)
                 {
                     IsNew = isNew,
                 };

                 projection.SetModelState(model);
                 return projection;
             });

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, cache, settings, logger);

        var result = await transformer.Transform(
                         CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "updated", TypeName = nameof(ItemUpdated), }),
                         CancellationToken.None);

        result.ShouldNotBeNull();
        result.IsNew.ShouldBeFalse();
        result.ExpectedEventNumber.ShouldBe(0);
        result.Model.Name.ShouldBe("updated");
        createFromModelIsNew.ShouldBe([false,]);
        A.CallTo(() => factory.Create(A<Guid>._, A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);

        await transformer.Transform(CreateEnvelope(modelId, 1, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);
        await transformer.Transform(CreateEnvelope(modelId, 2, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();

        transformer.ClearAll();

        await transformer.Transform(CreateEnvelope(modelId, 3, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedTwiceExactly();
    }

    private static ProjectionSettings<ItemModel> CreateSettings()
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
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };
    }

    private static EventEnvelope CreateEnvelope(Guid modelId, long eventNumber, Event @event)
    {
        @event.TypeName = string.IsNullOrWhiteSpace(@event.TypeName) ? @event.GetType().Name : @event.TypeName;
        @event.CreatedOn = @event.CreatedOn == default ? DateTime.UtcNow : @event.CreatedOn;

        return new EventEnvelope(
            eventNumber: eventNumber,
            streamPosition: new GlobalEventPosition((ulong)(eventNumber + 10)),
            streamId: $"item-{modelId}",
            modelId: modelId,
            createdOn: DateTime.UtcNow,
            @event: @event);
    }
}
