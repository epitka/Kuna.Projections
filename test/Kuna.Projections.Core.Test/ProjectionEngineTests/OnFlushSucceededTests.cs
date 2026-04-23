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

public class OnFlushSucceededTests
{
    [Fact]
    public async Task Should_Clear_Runtime_State_When_Used_Via_IProjectionLifecycle()
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
                 var projection = new ItemProjection(modelId) { IsNew = false, };
                 projection.ModelState.EventNumber = 0;
                 projection.ModelState.Name = "restored";
                 return new ValueTask<Projection<ItemModel>?>(projection);
             });

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);
        IProjectionLifecycle lifecycle = transformer;

        await transformer.Transform(
            CreateEnvelope(modelId, 0, new ItemCreated { Id = modelId, Name = "first", TypeName = nameof(ItemCreated), }),
            CancellationToken.None);

        createCalls.ShouldBe(1);

        lifecycle.OnFlushSucceeded([modelId,], [modelId,], new Dictionary<Guid, long?> { [modelId] = 0, });

        var second = await transformer.Transform(
                         CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "second", TypeName = nameof(ItemUpdated), }),
                         CancellationToken.None);

        second.ShouldNotBeNull();
        createCalls.ShouldBe(2);
        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task Should_Clear_Failed_Projection_Tracking_When_Used_Via_IProjectionLifecycle()
    {
        var factory = A.Fake<IProjectionFactory<ItemModel>>();
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>();
        var settings = CreateSettings(skipStateNotFoundFailure: false);
        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionEngine<ItemModel>>();

        var modelId = Guid.NewGuid();

        A.CallTo(() => factory.Create(modelId, true, A<CancellationToken>._))
         .Returns((Projection<ItemModel>?)null);

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);
        IProjectionLifecycle lifecycle = transformer;

        await transformer.Transform(CreateEnvelope(modelId, 1, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);
        await transformer.Transform(CreateEnvelope(modelId, 2, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();

        lifecycle.OnFlushSucceeded([modelId,], [modelId,], new Dictionary<Guid, long?>());

        await transformer.Transform(CreateEnvelope(modelId, 3, new TestEvent { TypeName = nameof(TestEvent), }), CancellationToken.None);

        A.CallTo(() => handler.Handle(A<ProjectionFailure>._, A<CancellationToken>._)).MustHaveHappenedTwiceExactly();
    }

    [Fact]
    public async Task Should_Not_Clear_Runtime_State_When_Projection_Advanced_Past_Flushed_Event()
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

        var transformer = new ProjectionEngine<ItemModel>(factory, handler, new InMemoryModelStateCache<ItemModel>(settings), settings, logger);
        IProjectionLifecycle lifecycle = transformer;

        await transformer.Transform(
            CreateEnvelope(modelId, 0, new ItemCreated { Id = modelId, Name = "first", TypeName = nameof(ItemCreated), }),
            CancellationToken.None);

        await transformer.Transform(
            CreateEnvelope(modelId, 1, new ItemUpdated { Id = modelId, Name = "second", TypeName = nameof(ItemUpdated), }),
            CancellationToken.None);

        lifecycle.OnFlushSucceeded([modelId,], [modelId,], new Dictionary<Guid, long?> { [modelId] = 0, });

        var third = await transformer.Transform(
                        CreateEnvelope(modelId, 2, new ItemUpdated { Id = modelId, Name = "third", TypeName = nameof(ItemUpdated), }),
                        CancellationToken.None);

        third.ShouldNotBeNull();
        third.Model.EventNumber.ShouldBe(2);
        createCalls.ShouldBe(1);
    }

    private static ProjectionSettings<ItemModel> CreateSettings(bool skipStateNotFoundFailure = true)
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
            SkipStateNotFoundFailure = skipStateNotFoundFailure,
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
