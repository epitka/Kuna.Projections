using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.InMemoryModelStateCacheTests;

public class SetTests
{
    [Fact]
    public void Set_Should_Preserve_State_And_Return_Clone_With_IsNew_Cleared()
    {
        var cache = new InMemoryModelStateCache<ItemModel>(CreateSettings());
        var modelId = Guid.NewGuid();
        var state = CreateState(
            modelId,
            eventNumber: 7,
            globalEventPosition: 42,
            name: "persisted",
            isNew: true,
            expectedEventNumber: 6);

        cache.Set(state);

        var found = cache.TryGet(modelId, out var cached);

        found.ShouldBeTrue();
        cached.ShouldNotBeNull();
        cached.Model.Id.ShouldBe(modelId);
        cached.Model.EventNumber.ShouldBe(7);
        cached.Model.GlobalEventPosition.ShouldBe(new GlobalEventPosition(42));
        cached.Model.Name.ShouldBe("persisted");
        cached.IsNew.ShouldBeFalse();
        cached.ShouldDelete.ShouldBeFalse();
        cached.GlobalEventPosition.ShouldBe(new GlobalEventPosition(42));
        cached.ExpectedEventNumber.ShouldBe(6);
        ReferenceEquals(cached.Model, state.Model).ShouldBeFalse();

        cached.Model.Name = "mutated";

        cache.TryGet(modelId, out var cachedAgain).ShouldBeTrue();
        cachedAgain.ShouldNotBeNull();
        cachedAgain.Model.Name.ShouldBe("persisted");
    }

    [Fact]
    public void Set_Should_Evict_Older_Entries_When_Capacity_Is_Exceeded()
    {
        var cache = new InMemoryModelStateCache<ItemModel>(CreateSettings(minEntries: 1, capacityMultiplier: 1, modelCountFlushThreshold: 1));
        var firstModelId = Guid.NewGuid();
        var secondModelId = Guid.NewGuid();

        cache.Set(CreateState(firstModelId, eventNumber: 0, globalEventPosition: 1, name: "first"));
        cache.Set(CreateState(secondModelId, eventNumber: 0, globalEventPosition: 2, name: "second"));

        cache.TryGet(firstModelId, out _).ShouldBeFalse();
        cache.TryGet(secondModelId, out var second).ShouldBeTrue();
        second.ShouldNotBeNull();
        second.Model.Name.ShouldBe("second");
    }

    [Fact]
    public void Set_Should_Not_Evict_Newer_State_For_Same_Model_When_Older_Eviction_Record_Is_Dequeued()
    {
        var cache = new InMemoryModelStateCache<ItemModel>(CreateSettings(minEntries: 2, capacityMultiplier: 1, modelCountFlushThreshold: 2));
        var modelId = Guid.NewGuid();
        var otherModelId = Guid.NewGuid();

        cache.Set(CreateState(modelId, eventNumber: 0, globalEventPosition: 1, name: "first"));
        cache.Set(CreateState(modelId, eventNumber: 1, globalEventPosition: 2, name: "newer"));
        cache.Set(CreateState(otherModelId, eventNumber: 0, globalEventPosition: 3, name: "other"));

        cache.TryGet(modelId, out var cached).ShouldBeTrue();
        cached.ShouldNotBeNull();
        cached.Model.EventNumber.ShouldBe(1);
        cached.Model.Name.ShouldBe("newer");
    }

    private static ProjectionSettings<ItemModel> CreateSettings(
        int minEntries = 10000,
        int capacityMultiplier = 3,
        int modelCountFlushThreshold = 100)
    {
        return new ProjectionSettings<ItemModel>
        {
            CatchUpFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.ModelCountBatching,
                ModelCountThreshold = modelCountFlushThreshold,
            },
            LiveProcessingFlush = new ProjectionFlushSettings
            {
                Strategy = PersistenceStrategy.TimeBasedBatching,
                ModelCountThreshold = modelCountFlushThreshold,
                Delay = 1000,
            },
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = minEntries,
            InFlightModelCacheCapacityMultiplier = capacityMultiplier,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };
    }

    private static ModelState<ItemModel> CreateState(
        Guid modelId,
        long eventNumber,
        ulong globalEventPosition,
        string name,
        bool isNew = false,
        long? expectedEventNumber = null)
    {
        return new ModelState<ItemModel>(
            new ItemModel
            {
                Id = modelId,
                EventNumber = eventNumber,
                GlobalEventPosition = new GlobalEventPosition(globalEventPosition),
                Name = name,
            },
            IsNew: isNew,
            ShouldDelete: false,
            GlobalEventPosition: new GlobalEventPosition(globalEventPosition),
            ExpectedEventNumber: expectedEventNumber);
    }
}
