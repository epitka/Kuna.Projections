using Bogus;
using DeepEqual.Syntax;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test;

public class InMemoryProjectionCacheTests
{
    [Fact]
    public async Task Stage_And_Get_Should_Preserve_State_And_Return_Clone()
    {
        var settings = CreateSettings();
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var modelId = Guid.NewGuid();
        var faker = new Faker();
        var envelopePosition = new GlobalEventPosition(faker.Random.ULong(1, 10_000));
        var modelPosition = new GlobalEventPosition(faker.Random.ULong(1, 10_000));
        var stageToken = faker.Random.Long(1, 1000);
        var stagedModel = new ItemModel
        {
            Id = Guid.Empty,
            Name = faker.Commerce.ProductName(),
            EventNumber = faker.Random.Long(1, 1000),
            GlobalEventPosition = modelPosition,
            HasStreamProcessingFaulted = faker.Random.Bool(),
        };

        var staged = new ProjectedStateEnvelope<ItemModel>(
            stagedModel,
            IsNew: faker.Random.Bool(),
            ShouldDelete: faker.Random.Bool(),
            GlobalEventPosition: envelopePosition,
            ExpectedEventNumber: faker.Random.Bool() ? null : faker.Random.Long(0, 999),
            StageToken: stageToken,
            PersistenceStatus: ProjectionPersistenceStatus.Dirty);

        await cache.Stage(staged, CancellationToken.None);

        var cached = await cache.Get(modelId, CancellationToken.None);

        cached.ShouldNotBeNull();
        cached.ShouldDeepEqual(staged);

        ReferenceEquals(cached.Model, staged.Model).ShouldBeFalse();

        cached.Model.Name = "mutated";

        var cachedAgain = await cache.Get(modelId, CancellationToken.None);
        cachedAgain.ShouldNotBeNull();
        cachedAgain.ShouldDeepEqual(staged);
    }

    private static ProjectionSettings<ItemModel> CreateSettings()
    {
        return new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.TimeBasedBatching,
            MaxPendingProjectionsCount = 100,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = 10000,
            InFlightModelCacheCapacityMultiplier = 3,
            EventVersionCheckStrategy = EventVersionCheckStrategy.Consecutive,
        };
    }
}
