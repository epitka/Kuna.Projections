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
            Id = modelId,
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
        Anonymize(cached).ShouldDeepEqual(Anonymize(staged));

        ReferenceEquals(cached.Model, staged.Model).ShouldBeFalse();

        cached.Model.Name = "mutated";

        var cachedAgain = await cache.Get(modelId, CancellationToken.None);
        cachedAgain.ShouldNotBeNull();
        Anonymize(cachedAgain).ShouldDeepEqual(Anonymize(staged));
    }

    [Fact]
    public async Task Pull_And_Complete_Should_Mark_Item_Persisted_And_Clear_IsNew()
    {
        var settings = CreateSettings();
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var staged = CreateStagedEnvelope();

        await cache.Stage(staged, CancellationToken.None);

        var batch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);

        batch.ShouldNotBeNull();
        batch.Items.Count.ShouldBe(1);
        batch.Items[0].PersistenceStatus.ShouldBe(ProjectionPersistenceStatus.InFlight);

        await cache.CompletePull(
            batch,
            [
                new PersistenceItemOutcome(
                    staged.Model.Id,
                    staged.StageToken,
                    staged.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        var cached = await cache.Get(staged.Model.Id, CancellationToken.None);

        cached.ShouldNotBeNull();
        cached.ShouldDeepEqual(
            staged with
            {
                IsNew = false,
                PersistenceStatus = ProjectionPersistenceStatus.Persisted,
            });
    }

    [Fact]
    public async Task CompletePull_Should_Ignore_Stale_Outcome_For_Newer_Staged_State()
    {
        var settings = CreateSettings();
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var first = CreateStagedEnvelope();
        var second = first with
        {
            Model = new ItemModel
            {
                Id = first.Model.Id,
                Name = "newer",
                EventNumber = first.Model.EventNumber + 1,
                GlobalEventPosition = new GlobalEventPosition(first.Model.GlobalEventPosition.Value + 1),
                HasStreamProcessingFaulted = first.Model.HasStreamProcessingFaulted,
            },
            GlobalEventPosition = new GlobalEventPosition(first.GlobalEventPosition.Value + 1),
            StageToken = first.StageToken + 1,
        };

        await cache.Stage(first, CancellationToken.None);
        var batch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);
        batch.ShouldNotBeNull();

        await cache.Stage(second, CancellationToken.None);

        await cache.CompletePull(
            batch,
            [
                new PersistenceItemOutcome(
                    first.Model.Id,
                    first.StageToken,
                    first.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        var cached = await cache.Get(first.Model.Id, CancellationToken.None);

        cached.ShouldNotBeNull();
        cached.ShouldDeepEqual(second);
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

    private static ProjectedStateEnvelope<ItemModel> CreateStagedEnvelope()
    {
        var faker = new Faker();
        var envelopePosition = new GlobalEventPosition(faker.Random.ULong(1, 10_000));
        var modelPosition = new GlobalEventPosition(faker.Random.ULong(1, 10_000));
        var modelId = Guid.NewGuid();

        return new ProjectedStateEnvelope<ItemModel>(
            new ItemModel
            {
                Id = modelId,
                Name = faker.Commerce.ProductName(),
                EventNumber = faker.Random.Long(1, 1000),
                GlobalEventPosition = modelPosition,
                HasStreamProcessingFaulted = faker.Random.Bool(),
            },
            IsNew: true,
            ShouldDelete: false,
            GlobalEventPosition: envelopePosition,
            ExpectedEventNumber: faker.Random.Bool() ? null : faker.Random.Long(0, 999),
            StageToken: faker.Random.Long(1, 1000),
            PersistenceStatus: ProjectionPersistenceStatus.Dirty);
    }

    private static ProjectedStateEnvelope<ItemModel> Anonymize(ProjectedStateEnvelope<ItemModel> state)
    {
        return state with
        {
            Model = new ItemModel
            {
                Id = Guid.Empty,
                Name = state.Model.Name,
                EventNumber = state.Model.EventNumber,
                GlobalEventPosition = state.Model.GlobalEventPosition,
                HasStreamProcessingFaulted = state.Model.HasStreamProcessingFaulted,
            },
        };
    }
}
