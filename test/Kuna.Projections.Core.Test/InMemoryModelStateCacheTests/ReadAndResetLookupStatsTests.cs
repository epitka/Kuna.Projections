using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.InMemoryModelStateCacheTests;

public class ReadAndResetLookupStatsTests
{
    [Fact]
    public void ReadAndResetLookupStats_Should_Return_And_Reset_Hits_And_Misses()
    {
        var cache = new InMemoryModelStateCache<ItemModel>(CreateSettings());
        var modelId = Guid.NewGuid();

        cache.Set(
            new ModelState<ItemModel>(
                new ItemModel
                {
                    Id = modelId,
                    EventNumber = 0,
                    GlobalEventPosition = new GlobalEventPosition("1"),
                    Name = "cached",
                },
                IsNew: true,
                ShouldDelete: false,
                GlobalEventPosition: new GlobalEventPosition("1"),
                ExpectedEventNumber: null));

        cache.TryGet(modelId, out _).ShouldBeTrue();
        cache.TryGet(Guid.NewGuid(), out _).ShouldBeFalse();

        cache.ReadAndResetLookupStats().ShouldBe((Hits: 1L, Misses: 1L));
        cache.ReadAndResetLookupStats().ShouldBe((Hits: 0L, Misses: 0L));
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
            ModelStateCacheCapacity = 10000,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };
    }
}
