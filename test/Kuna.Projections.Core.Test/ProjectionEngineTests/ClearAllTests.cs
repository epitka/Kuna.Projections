using FakeItEasy;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionEngineTests;

public class ClearAllTests
{
    [Fact]
    public async Task Should_Reset_Failed_Projection_Tracking()
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
