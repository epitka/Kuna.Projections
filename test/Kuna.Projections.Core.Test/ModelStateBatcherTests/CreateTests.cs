using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ModelStateBatcherTests;

public class CreateTests
{
    [Fact]
    public async Task Create_Should_Batch_Immediately_For_ImmediateModelFlush()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ImmediateModelFlush,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(2);
        batches[0].Changes.Count.ShouldBe(1);
        batches[1].Changes.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Create_Should_Group_By_Model_Count_And_Keep_Last_Change_Per_Model()
    {
        var modelId = Guid.NewGuid();
        var changes = new[]
        {
            Helpers.CreateChange(modelId, 10, "v1"),
            Helpers.CreateChange(Guid.NewGuid(), 11, "other"),
            Helpers.CreateChange(modelId, 12, "v2"),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 10,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(1);
        batches[0].Changes.Count.ShouldBe(2);
        batches[0].GlobalEventPosition.ShouldBe(new GlobalEventPosition("12"));
        batches[0].Changes.ShouldContain(x => x.Model.Id == modelId && x.Model.Name == "v2");
        batches[0].Changes.ShouldNotContain(x => x.Model.Id == modelId && x.Model.Name == "v1");
    }

    [Fact]
    public async Task Create_Should_Cap_ModelCountBatching_By_Distinct_Model_Ids()
    {
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var thirdId = Guid.NewGuid();
        var changes = new[]
        {
            Helpers.CreateChange(firstId, 1, "first-v1"),
            Helpers.CreateChange(firstId, 2, "first-v2"),
            Helpers.CreateChange(secondId, 3, "second"),
            Helpers.CreateChange(thirdId, 4, "third"),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 2,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(2);
        batches[0].Changes.Count.ShouldBe(2);
        batches[0].GlobalEventPosition.ShouldBe(new GlobalEventPosition("3"));
        batches[0].Changes.ShouldContain(x => x.Model.Id == firstId && x.Model.Name == "first-v2");
        batches[0].Changes.ShouldContain(x => x.Model.Id == secondId);
        batches[1].Changes.Count.ShouldBe(1);
        batches[1].Changes[0].Model.Id.ShouldBe(thirdId);
    }

    [Fact]
    public async Task Create_Should_Filter_Invalid_NewAndDelete_Changes()
    {
        var validId = Guid.NewGuid();
        var changes = new[]
        {
            new ModelState<ItemModel>(
                new ItemModel
                {
                    Id = Guid.NewGuid(),
                    EventNumber = 1,
                    GlobalEventPosition = new GlobalEventPosition("1"),
                },
                IsNew: true,
                ShouldDelete: true,
                GlobalEventPosition: new GlobalEventPosition("1"),
                ExpectedEventNumber: null),
            Helpers.CreateChange(validId, 2, "keep"),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 10,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(1);
        batches[0].Changes.Count.ShouldBe(1);
        batches[0].Changes[0].Model.Id.ShouldBe(validId);
        batches[0].GlobalEventPosition.ShouldBe(new GlobalEventPosition("2"));
    }

    [Fact]
    public async Task Create_Should_Flush_By_Time_For_TimeBasedBatching()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
            Helpers.CreateChange(Guid.NewGuid(), 3),
        };

        using var system = ActorSystem.Create("projection-batcher-time-test");
        var materializer = ActorMaterializer.Create(system);

        var settings = new TestProjectionSettings
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 100,
                Delay = 5,
            },
        };

        var result = await Source.From(changes)
                                 .Throttle(1, TimeSpan.FromMilliseconds(25), 1, ThrottleMode.Shaping)
                                 .Via(ModelStateBatcher.Create<ItemModel>(settings))
                                 .RunWith(Sink.Seq<ModelStatesBatch<ItemModel>>(), materializer);

        result.Count.ShouldBe(3);
        result.All(x => x.Changes.Count == 1).ShouldBeTrue();

        materializer.Shutdown();
        await system.Terminate();
    }

    [Fact]
    public async Task Create_Should_Normalize_NonPositive_ModelCountBatch_Size_To_One()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 0,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(2);
        batches.All(x => x.Changes.Count == 1).ShouldBeTrue();
    }

    [Fact]
    public async Task Create_Should_Not_Flush_TimeBasedBatching_By_Model_Count()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
        };

        using var system = ActorSystem.Create("projection-batcher-time-no-count-test");
        var materializer = ActorMaterializer.Create(system);

        var settings = new TestProjectionSettings
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 1,
                Delay = 1000,
            },
        };

        var result = await Source.From(changes)
                                 .Via(ModelStateBatcher.Create<ItemModel>(settings))
                                 .RunWith(Sink.Seq<ModelStatesBatch<ItemModel>>(), materializer);

        result.Count.ShouldBe(1);
        result[0].Changes.Count.ShouldBe(2);

        materializer.Shutdown();
        await system.Terminate();
    }

    [Fact]
    public async Task Create_Should_Normalize_NonPositive_TimeBatch_Settings()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
        };

        using var system = ActorSystem.Create("projection-batcher-time-normalize-test");
        var materializer = ActorMaterializer.Create(system);

        var settings = new TestProjectionSettings
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = 0,
                Delay = 0,
            },
        };

        var result = await Source.From(changes)
                                 .Throttle(1, TimeSpan.FromMilliseconds(25), 1, ThrottleMode.Shaping)
                                 .Via(ModelStateBatcher.Create<ItemModel>(settings))
                                 .RunWith(Sink.Seq<ModelStatesBatch<ItemModel>>(), materializer);

        result.Count.ShouldBe(2);
        result.All(x => x.Changes.Count == 1).ShouldBeTrue();

        materializer.Shutdown();
        await system.Terminate();
    }

    [Fact]
    public async Task Create_Should_Use_ModelCountBatching_For_Unknown_Strategy()
    {
        var changes = new[]
        {
            Helpers.CreateChange(Guid.NewGuid(), 1),
            Helpers.CreateChange(Guid.NewGuid(), 2),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = (PersistenceStrategy)int.MaxValue,
                                  ModelCountThreshold = 10,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(1);
        batches[0].Changes.Count.ShouldBe(2);
        batches[0].GlobalEventPosition.ShouldBe(new GlobalEventPosition("2"));
    }

    [Fact]
    public async Task Create_Should_Emit_Empty_Batch_When_All_Changes_Are_Filtered_Out()
    {
        var changes = new[]
        {
            new ModelState<ItemModel>(
                new ItemModel
                {
                    Id = Guid.NewGuid(),
                    EventNumber = 1,
                    GlobalEventPosition = new GlobalEventPosition("1"),
                },
                IsNew: true,
                ShouldDelete: true,
                GlobalEventPosition: new GlobalEventPosition("1"),
                ExpectedEventNumber: null),
        };

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 10,
                              },
                          },
                          changes);

        batches.Count.ShouldBe(1);
        batches[0].Changes.ShouldBeEmpty();
        batches[0].GlobalEventPosition.ShouldBe(default);
    }

    [Fact]
    public async Task Create_Should_Not_Emit_Batches_For_Empty_Source()
    {
        var changes = Array.Empty<ModelState<ItemModel>>();

        var batches = await Helpers.RunBatcher(
                          new TestProjectionSettings
                          {
                              CatchUpFlush = new ProjectionFlushSettings
                              {
                                  Strategy = PersistenceStrategy.ModelCountBatching,
                                  ModelCountThreshold = 10,
                              },
                          },
                          changes);

        batches.ShouldBeEmpty();
    }
}
