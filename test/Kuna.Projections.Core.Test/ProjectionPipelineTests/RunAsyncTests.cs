using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;
using Kuna.Projections.Core.Test.Shared.Events;
using Kuna.Projections.Core.Test.Shared.Models;
using Kuna.Projections.Core.Test.Shared.Projections;
using FakeItEasy;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests;

public class RunAsyncTests
{
    [Fact]
    public async Task Should_Continue_Transforming_While_First_Persist_Is_Blocked()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var envelopes = CreateEnvelopes(40);
        var source = new FastSource(envelopes);
        var runtime = new CountingEngineLike();
        var sink = new BlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            MaxPendingProjectionsCount = 16,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };

        var logger = LoggerFactory.Create(
                                      builder =>
                                      {
                                      })
                                  .CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>();

        var pipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            source,
            runtime,
            runtime,
            new InMemoryModelStateCache<ItemModel>(settings),
            sink,
            checkpointStore,
            settings,
            logger);

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(testCancellationToken);
        var runTask = pipeline.RunAsync(linkedCts.Token);

        await sink.FirstPersistStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), testCancellationToken);
        await Task.Delay(150, testCancellationToken);

        runtime.TransformedCount.ShouldBeGreaterThan(1);
        sink.PersistCalls.ShouldBe(1);

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        runtime.TransformedCount.ShouldBe(envelopes.Count);
        sink.PersistCalls.ShouldBe(2);
    }

    [Fact]
    public async Task EndToEnd_Should_Apply_Real_Projection_And_Persist_Final_State()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var modelId = Guid.NewGuid();
        var envelopes = CreateItemProjectionEnvelopes(modelId);
        var source = new FastSource(envelopes);
        var stateStore = new NullStateStore();
        var projectionFactory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            MaxPendingProjectionsCount = 100,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };

        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var engine = new ProjectionEngine<ItemModel>(
            projectionFactory,
            new NoOpFailureHandler(),
            new InMemoryModelStateCache<ItemModel>(settings),
            settings,
            loggerFactory.CreateLogger<ProjectionEngine<ItemModel>>());

        var sink = new CapturingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var pipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            source,
            engine,
            engine,
            new InMemoryModelStateCache<ItemModel>(settings),
            sink,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(testCancellationToken);
        await pipeline.RunAsync(linkedCts.Token);

        sink.Batches.Count.ShouldBe(1);
        sink.Batches[0].Items.Count.ShouldBe(1);
        var persisted = sink.Batches[0].Items[0].Model;
        persisted.Id.ShouldBe(modelId);
        persisted.EventNumber.ShouldBe(2);
        persisted.Name.ShouldBe("third");
    }

    [Fact]
    public async Task Should_Snapshot_Model_State_Before_A_Blocked_Flush_While_Later_Events_Keep_Transforming()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var modelId = Guid.NewGuid();
        var envelopes = CreateItemProjectionEnvelopes(modelId);
        var source = new FastSource(envelopes);
        var stateStore = new NullStateStore();
        var projectionFactory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            MaxPendingProjectionsCount = 16,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };

        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var sharedCache = new InMemoryModelStateCache<ItemModel>(settings);
        var engine = new ProjectionEngine<ItemModel>(
            projectionFactory,
            new NoOpFailureHandler(),
            sharedCache,
            settings,
            loggerFactory.CreateLogger<ProjectionEngine<ItemModel>>());

        var sink = new CapturingBlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var pipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            source,
            engine,
            engine,
            sharedCache,
            sink,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(testCancellationToken);
        var runTask = pipeline.RunAsync(linkedCts.Token);

        await sink.FirstPersistStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), testCancellationToken);
        await Task.Delay(150, testCancellationToken);

        sink.Batches.Count.ShouldBe(1);
        sink.Batches[0].Items.Count.ShouldBe(1);
        sink.Batches[0].Items[0].Model.EventNumber.ShouldBe(0);
        sink.Batches[0].Items[0].Model.Name.ShouldBe("first");

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        sink.Batches.Count.ShouldBe(2);
        sink.Batches[1].Items.Count.ShouldBe(1);
        sink.Batches[1].Items[0].Model.EventNumber.ShouldBe(2);
        sink.Batches[1].Items[0].Model.Name.ShouldBe("third");
        sink.Batches[1].Items[0].ExpectedEventNumber.ShouldBe(0);
    }

    [Fact]
    public async Task Should_Not_Clear_Runtime_State_For_A_Model_With_Newer_Pending_Changes()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var modelId = Guid.NewGuid();
        var factory = A.Fake<IProjectionFactory<ItemModel>>();
        var handler = A.Fake<IProjectionFailureHandler<ItemModel>>(opt => opt.Strict());
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.ImmediateModelFlush,
            MaxPendingProjectionsCount = 16,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };

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

        var source = new FastSource(CreateItemProjectionEnvelopes(modelId));
        var sharedCache = new InMemoryModelStateCache<ItemModel>(settings);
        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var engine = new ProjectionEngine<ItemModel>(
            factory,
            handler,
            sharedCache,
            settings,
            loggerFactory.CreateLogger<ProjectionEngine<ItemModel>>());

        var sink = new CapturingBlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var pipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            source,
            engine,
            engine,
            sharedCache,
            sink,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(testCancellationToken);
        var runTask = pipeline.RunAsync(linkedCts.Token);

        await sink.FirstPersistStarted.Task.WaitAsync(TimeSpan.FromSeconds(5), testCancellationToken);
        await Task.Delay(150, testCancellationToken);

        createCalls.ShouldBe(1);

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        createCalls.ShouldBe(1);
        sink.Batches.Count.ShouldBe(2);
        sink.Batches[1].Items[0].ExpectedEventNumber.ShouldBe(0);
    }

    [Fact]
    public async Task Partial_Batch_Failure_Should_Advance_Checkpoint_And_Not_Repull_Failed_Items()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var failedModelId = Guid.NewGuid();
        var persistedModelId = Guid.NewGuid();
        var laterModelId = Guid.NewGuid();
        var source = new FastSource(
            [
                CreateEnvelope(failedModelId, 0, 10, new ItemCreated { Id = failedModelId, Name = "failed", TypeName = nameof(ItemCreated), }),
                CreateEnvelope(persistedModelId, 0, 11, new ItemCreated { Id = persistedModelId, Name = "persisted", TypeName = nameof(ItemCreated), }),
                CreateEnvelope(laterModelId, 0, 12, new ItemCreated { Id = laterModelId, Name = "later", TypeName = nameof(ItemCreated), }),
            ]);
        var stateStore = new NullStateStore();
        var projectionFactory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            MaxPendingProjectionsCount = 2,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };
        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });
        var sharedCache = new InMemoryModelStateCache<ItemModel>(settings);
        var engine = new ProjectionEngine<ItemModel>(
            projectionFactory,
            new NoOpFailureHandler(),
            sharedCache,
            settings,
            loggerFactory.CreateLogger<ProjectionEngine<ItemModel>>());
        var sink = new MixedOutcomeSink(failedModelId);
        var checkpointStore = new InMemoryCheckpointStore();
        var pipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            source,
            engine,
            engine,
            sharedCache,
            sink,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        await pipeline.RunAsync(testCancellationToken);

        // The first flush contains both models, but only one fails.
        sink.Batches.Count.ShouldBe(2);
        sink.Batches[0].Items.Select(x => x.Model.Id).ShouldBe([failedModelId, persistedModelId], ignoreOrder: true);

        // The failed model stays completed-in-cache and must not be pulled again
        // on the later flush triggered by the third event.
        sink.Batches[1].Items.Select(x => x.Model.Id).ShouldBe([laterModelId,]);

        // Processing continues and checkpoint advances to the latest observed
        // position even though one item in the earlier batch failed.
        var checkpoint = await checkpointStore.GetCheckpoint(testCancellationToken);
        checkpoint.GlobalEventPosition.ShouldBe(new GlobalEventPosition(12));

        var failedCached = await sharedCache.Get(failedModelId, testCancellationToken);
        failedCached.ShouldNotBeNull();
        failedCached.PersistenceStatus.ShouldBe(ProjectionPersistenceStatus.Failed);

        var laterCached = await sharedCache.Get(laterModelId, testCancellationToken);
        laterCached.ShouldNotBeNull();
        laterCached.PersistenceStatus.ShouldBe(ProjectionPersistenceStatus.Persisted);
    }

    private static IReadOnlyList<EventEnvelope> CreateEnvelopes(int count)
    {
        var list = new List<EventEnvelope>(count);

        for (var i = 0; i < count; i++)
        {
            var modelId = Guid.NewGuid();
            list.Add(
                new EventEnvelope(
                    eventNumber: i,
                    streamPosition: new GlobalEventPosition((ulong)i),
                    streamId: $"item-{modelId}",
                    @event: new TestEvent
                    {
                        TypeName = nameof(TestEvent),
                        CreatedOn = DateTime.UtcNow,
                    },
                    modelId: modelId,
                    createdOn: DateTime.UtcNow));
        }

        return list;
    }

    private static IReadOnlyList<EventEnvelope> CreateItemProjectionEnvelopes(Guid modelId)
    {
        var created = new ItemCreated
        {
            Id = modelId,
            Name = "first",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var updated1 = new ItemUpdated
        {
            Id = modelId,
            Name = "second",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var updated2 = new ItemUpdated
        {
            Id = modelId,
            Name = "third",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        return new[]
        {
            new EventEnvelope(0, new GlobalEventPosition(1), $"item-{modelId}", created, modelId, created.CreatedOn),
            new EventEnvelope(1, new GlobalEventPosition(2), $"item-{modelId}", updated1, modelId, updated1.CreatedOn),
            new EventEnvelope(2, new GlobalEventPosition(3), $"item-{modelId}", updated2, modelId, updated2.CreatedOn),
        };
    }

    private static EventEnvelope CreateEnvelope(Guid modelId, long eventNumber, ulong streamPosition, Event @event)
    {
        @event.TypeName = string.IsNullOrWhiteSpace(@event.TypeName) ? @event.GetType().Name : @event.TypeName;
        @event.CreatedOn = @event.CreatedOn == default ? DateTime.UtcNow : @event.CreatedOn;

        return new EventEnvelope(
            eventNumber,
            new GlobalEventPosition(streamPosition),
            $"item-{modelId}",
            @event,
            modelId,
            @event.CreatedOn);
    }
}
