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
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                ModelCountThreshold = 16,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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
        sink.PersistCalls.ShouldBe(envelopes.Count);
    }

    [Fact]
    public async Task Should_Stop_Pipeline_On_Cancellation_But_Let_InFlight_Persist_Finish()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var envelopes = CreateEnvelopes(40);
        var source = new FastSource(envelopes);
        var runtime = new CountingEngineLike();
        var sink = new CapturingBlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                ModelCountThreshold = 16,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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

        linkedCts.Cancel();
        await Task.Delay(150, testCancellationToken);

        runTask.IsCompleted.ShouldBeFalse();
        sink.Batches.Count.ShouldBe(1);

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        sink.Batches.Count.ShouldBe(1);
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
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 100,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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
            new ProjectionCreationRegistration<ItemModel>(typeof(ItemCreated)),
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
        sink.Batches[0].Changes.Count.ShouldBe(1);
        var persisted = sink.Batches[0].Changes[0].Model;
        persisted.Id.ShouldBe(modelId);
        persisted.EventNumber.ShouldBe(2);
        persisted.Name.ShouldBe("third");
    }

    [Fact]
    public async Task Should_Use_LiveProcessing_ModelCountFlushThreshold_After_CaughtUp()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var catchUpEnvelope = CreateEnvelopes(1)[0];
        var liveEnvelopes = CreateEnvelopes(3);
        var source = new FastSource(
            new[]
                {
                    catchUpEnvelope,
                    CreateCaughtUpEnvelope(),
                }.Concat(liveEnvelopes)
                 .ToArray());

        var runtime = new CountingEngineLike();
        var sink = new CapturingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 100,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 2,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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

        await pipeline.RunAsync(testCancellationToken);

        sink.Batches.Select(x => x.Changes.Count).ShouldBe([1, 2, 1,]);
    }

    [Fact]
    public async Task EndToEnd_Delete_Should_Persist_Delete_But_Not_Publish_Deleted_State_To_Cache()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var modelId = Guid.NewGuid();
        var createEnvelopes = new EventEnvelope[]
        {
            new(
                eventNumber: 0,
                streamPosition: new GlobalEventPosition(10),
                streamId: $"item-{modelId}",
                modelId: modelId,
                createdOn: DateTime.UtcNow,
                @event: new ItemCreated
                {
                    Id = modelId,
                    Name = "first",
                    TypeName = nameof(ItemCreated),
                    CreatedOn = DateTime.UtcNow,
                }),
        };

        var deleteEnvelopes = new EventEnvelope[]
        {
            new(
                eventNumber: 1,
                streamPosition: new GlobalEventPosition(11),
                streamId: $"item-{modelId}",
                modelId: modelId,
                createdOn: DateTime.UtcNow,
                @event: new ItemDeleted
                {
                    Id = modelId,
                    TypeName = nameof(ItemDeleted),
                    CreatedOn = DateTime.UtcNow,
                }),
        };

        var stateStore = new InMemoryStateStoreSink();
        var projectionFactory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 100,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };

        var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var engineCache = new InMemoryModelStateCache<ItemModel>(settings);
        var pipelineCache = new InMemoryModelStateCache<ItemModel>(settings);
        var engine = new ProjectionEngine<ItemModel>(
            projectionFactory,
            new NoOpFailureHandler(),
            engineCache,
            new ProjectionCreationRegistration<ItemModel>(typeof(ItemCreated)),
            settings,
            loggerFactory.CreateLogger<ProjectionEngine<ItemModel>>());

        var checkpointStore = new InMemoryCheckpointStore();
        var createPipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            new FastSource(createEnvelopes),
            engine,
            engine,
            new InMemoryModelStateCache<ItemModel>(settings),
            stateStore,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        await createPipeline.RunAsync(testCancellationToken);

        var deletePipeline = new ProjectionPipeline<EventEnvelope, ItemModel>(
            new FastSource(deleteEnvelopes),
            engine,
            engine,
            pipelineCache,
            stateStore,
            checkpointStore,
            settings,
            loggerFactory.CreateLogger<ProjectionPipeline<EventEnvelope, ItemModel>>());

        await deletePipeline.RunAsync(testCancellationToken);

        stateStore.Batches.Count.ShouldBe(2);
        stateStore.Batches[1].Changes.Count.ShouldBe(1);
        stateStore.Batches[1].Changes[0].ShouldDelete.ShouldBeTrue();
        pipelineCache.TryGet(modelId, out _).ShouldBeFalse();
    }

    [Fact]
    public async Task Should_Cap_Sink_Batches_And_Backpressure_Transform_When_Sink_Is_Blocked()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var envelopes = CreateEnvelopes(100);
        var source = new FastSource(envelopes);
        var runtime = new CountingEngineLike();
        var sink = new CapturingBlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 3,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                Delay = 1000,
            },
            Backpressure = new ProjectionBackpressureSettings
            {
                SourceToTransformBufferCapacity = 6,
                TransformToSinkBufferCapacity = 6,
            },
            ModelStateCacheCapacity = 10000,
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

        sink.Batches.Count.ShouldBe(1);
        sink.Batches[0].Changes.Count.ShouldBe(3);
        runtime.TransformedCount.ShouldBeLessThan(40);

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        var expectedBatchSizes = Enumerable.Repeat(3, 33).Append(1).ToArray();

        runtime.TransformedCount.ShouldBe(envelopes.Count);
        sink.Batches.All(x => x.Changes.Count <= 3).ShouldBeTrue();
        sink.Batches.Select(x => x.Changes.Count).ShouldBe(expectedBatchSizes);
    }

    [Fact]
    public async Task Should_Flush_TimeBasedBatching_While_Source_Continues_Producing()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var modelId = Guid.NewGuid();
        var envelopes = CreateEnvelopesForModel(modelId, 200);
        var source = new DelayedSource(envelopes, TimeSpan.FromMilliseconds(10));
        var runtime = new CountingEngineLike();
        var sink = new CapturingBlockingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 1000,
                Delay = 25,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                Delay = 25,
            },
            Backpressure = new ProjectionBackpressureSettings
            {
                SourceToTransformBufferCapacity = 4,
                TransformToSinkBufferCapacity = 4,
            },
            ModelStateCacheCapacity = 10000,
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

        runtime.TransformedCount.ShouldBeLessThan(settings.CatchUpFlush.ModelCountThreshold);
        sink.Batches.Count.ShouldBe(1);
        sink.Batches[0].Changes.Count.ShouldBe(1);

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);
    }

    [Fact]
    public async Task Should_Not_Use_ModelCountThreshold_For_TimeBasedBatching()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var envelopes = CreateEnvelopes(3);
        var source = new FastSource(envelopes);
        var runtime = new CountingEngineLike();
        var sink = new CapturingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 1,
                Delay = 1000,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 1,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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

        await pipeline.RunAsync(testCancellationToken);

        sink.Batches.Count.ShouldBe(1);
        sink.Batches[0].Changes.Count.ShouldBe(3);
    }

    [Fact]
    public async Task Should_Clear_Lifecycle_State_For_Flushed_Models()
    {
        var testCancellationToken = TestContext.Current.CancellationToken;
        var envelopes = CreateEnvelopes(6);
        var source = new FastSource(envelopes);
        var runtime = new CountingEngineLike();
        var sink = new CapturingSink();
        var checkpointStore = new InMemoryCheckpointStore();
        var settings = new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = 3,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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

        await pipeline.RunAsync(testCancellationToken);

        runtime.ClearedModelIds.OrderBy(x => x).ShouldBe(envelopes.Select(x => x.ModelId).OrderBy(x => x));
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
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                ModelCountThreshold = 16,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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
            new ProjectionCreationRegistration<ItemModel>(typeof(ItemCreated)),
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
        sink.Batches[0].Changes.Count.ShouldBe(1);
        sink.Batches[0].Changes[0].Model.EventNumber.ShouldBe(0);
        sink.Batches[0].Changes[0].Model.Name.ShouldBe("first");

        sink.ReleaseFirstPersist();
        await runTask.WaitAsync(TimeSpan.FromSeconds(10), testCancellationToken);

        sink.Batches.Count.ShouldBe(3);
        sink.Batches[1].Changes.Count.ShouldBe(1);
        sink.Batches[1].Changes[0].Model.EventNumber.ShouldBe(1);
        sink.Batches[1].Changes[0].Model.Name.ShouldBe("second");
        sink.Batches[1].Changes[0].ExpectedEventNumber.ShouldBe(0);
        sink.Batches[2].Changes.Count.ShouldBe(1);
        sink.Batches[2].Changes[0].Model.EventNumber.ShouldBe(2);
        sink.Batches[2].Changes[0].Model.Name.ShouldBe("third");
        sink.Batches[2].Changes[0].ExpectedEventNumber.ShouldBe(1);
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
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                ModelCountThreshold = 16,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ImmediateModelFlush,
                Delay = 1000,
            },
            ModelStateCacheCapacity = 10000,
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
            new ProjectionCreationRegistration<ItemModel>(typeof(ItemCreated)),
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
        sink.Batches.Count.ShouldBe(3);
        sink.Batches[1].Changes[0].ExpectedEventNumber.ShouldBe(0);
        sink.Batches[2].Changes[0].ExpectedEventNumber.ShouldBe(1);
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

    private static IReadOnlyList<EventEnvelope> CreateEnvelopesForModel(Guid modelId, int count)
    {
        var list = new List<EventEnvelope>(count);

        for (var i = 0; i < count; i++)
        {
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

    private static EventEnvelope CreateCaughtUpEnvelope()
    {
        var caughtUp = new ProjectionCaughtUpEvent();

        return new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(0),
            streamId: "$projection-caught-up",
            @event: caughtUp,
            modelId: Guid.Empty,
            createdOn: caughtUp.CreatedOn);
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
}
