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
        var stagedVersionToken = faker.Random.Long(1, 1000);
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
            StagedVersionToken: stagedVersionToken,
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
                    staged.StagedVersionToken,
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
            StagedVersionToken = first.StagedVersionToken + 1,
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
                    first.StagedVersionToken,
                    first.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        var cached = await cache.Get(first.Model.Id, CancellationToken.None);

        cached.ShouldNotBeNull();
        cached.ShouldDeepEqual(second);
    }

    [Fact]
    public async Task Persisted_Entries_Should_Become_Evictable_When_Capacity_Is_Exceeded()
    {
        var settings = CreateSettings(minEntries: 1, capacityMultiplier: 1, maxPending: 1);
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var first = CreateStagedEnvelope();
        var second = CreateStagedEnvelope();

        // The first item becomes persisted and therefore evictable.
        await cache.Stage(first, CancellationToken.None);
        var firstBatch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);
        firstBatch.ShouldNotBeNull();
        await cache.CompletePull(
            firstBatch,
            [
                new PersistenceItemOutcome(
                    first.Model.Id,
                    first.StagedVersionToken,
                    first.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        // Persisting a second item above capacity should evict the older
        // persisted entry, while keeping the newer persisted entry available.
        await cache.Stage(second, CancellationToken.None);
        var secondBatch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);
        secondBatch.ShouldNotBeNull();
        await cache.CompletePull(
            secondBatch,
            [
                new PersistenceItemOutcome(
                    second.Model.Id,
                    second.StagedVersionToken,
                    second.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        var firstCached = await cache.Get(first.Model.Id, CancellationToken.None);
        var secondCached = await cache.Get(second.Model.Id, CancellationToken.None);

        firstCached.ShouldBeNull();
        secondCached.ShouldNotBeNull();
        secondCached.ShouldDeepEqual(
            second with
            {
                IsNew = false,
                PersistenceStatus = ProjectionPersistenceStatus.Persisted,
            });
    }

    [Fact]
    public async Task Failed_Entries_Should_Remain_In_Cache_Even_When_Capacity_Is_Exceeded()
    {
        var settings = CreateSettings(minEntries: 1, capacityMultiplier: 1, maxPending: 1);
        var cache = new InMemoryModelStateCache<ItemModel>(settings);
        var failed = CreateStagedEnvelope();
        var persisted = CreateStagedEnvelope();

        // Failed entries stay protected in cache and are not repulled.
        await cache.Stage(failed, CancellationToken.None);
        var failedBatch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);
        failedBatch.ShouldNotBeNull();
        await cache.CompletePull(
            failedBatch,
            [
                new PersistenceItemOutcome(
                    failed.Model.Id,
                    failed.StagedVersionToken,
                    failed.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Failed,
                    null),
            ],
            CancellationToken.None);

        // When capacity pressure arrives later, the persisted entry is the one
        // that can be dropped while the failed entry remains retained.
        await cache.Stage(persisted, CancellationToken.None);
        var persistedBatch = await cache.PullNextBatch(new PersistencePullRequest { MaxBatchSize = 1, }, CancellationToken.None);
        persistedBatch.ShouldNotBeNull();
        await cache.CompletePull(
            persistedBatch,
            [
                new PersistenceItemOutcome(
                    persisted.Model.Id,
                    persisted.StagedVersionToken,
                    persisted.GlobalEventPosition,
                    PersistenceItemOutcomeStatus.Persisted,
                    null),
            ],
            CancellationToken.None);

        var failedCached = await cache.Get(failed.Model.Id, CancellationToken.None);
        var persistedCached = await cache.Get(persisted.Model.Id, CancellationToken.None);

        failedCached.ShouldNotBeNull();
        failedCached.ShouldDeepEqual(
            failed with
            {
                PersistenceStatus = ProjectionPersistenceStatus.Failed,
            });

        persistedCached.ShouldBeNull();
    }

    private static ProjectionSettings<ItemModel> CreateSettings(
        int minEntries = 10000,
        int capacityMultiplier = 3,
        int maxPending = 100)
    {
        return new ProjectionSettings<ItemModel>
        {
            CatchUpPersistenceStrategy = PersistenceStrategy.ModelCountBatching,
            LiveProcessingPersistenceStrategy = PersistenceStrategy.TimeBasedBatching,
            MaxPendingProjectionsCount = maxPending,
            LiveProcessingFlushDelay = 1000,
            SkipStateNotFoundFailure = true,
            InFlightModelCacheMinEntries = minEntries,
            InFlightModelCacheCapacityMultiplier = capacityMultiplier,
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
            StagedVersionToken: faker.Random.Long(1, 1000),
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
