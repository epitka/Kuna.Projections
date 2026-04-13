using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class WriteBatchTests : DataStoreIntegrationTestBase
{
    public WriteBatchTests(PostgresSqlContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task InvalidInsert_Should_Return_Mixed_Item_Outcomes()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var validId = Guid.NewGuid();
        var invalidId = Guid.NewGuid();

        var outcomes = await store.WriteBatch(
                           new PersistenceWriteBatch<TestModel>
                           {
                               Items =
                               [
                                   new ProjectedStateEnvelope<TestModel>(
                                       new TestModel
                                       {
                                           Id = validId,
                                           Name = "valid",
                                           EventNumber = 1,
                                           GlobalEventPosition = new GlobalEventPosition(10),
                                           HasStreamProcessingFaulted = false,
                                       },
                                       IsNew: true,
                                       ShouldDelete: false,
                                       GlobalEventPosition: new GlobalEventPosition(10),
                                       ExpectedEventNumber: null,
                                       StageToken: 1,
                                       PersistenceStatus: ProjectionPersistenceStatus.InFlight),
                                   new ProjectedStateEnvelope<TestModel>(
                                       new TestModel
                                       {
                                           Id = invalidId,
                                           Name = new string('x', 128),
                                           EventNumber = 2,
                                           GlobalEventPosition = new GlobalEventPosition(11),
                                           HasStreamProcessingFaulted = false,
                                       },
                                       IsNew: true,
                                       ShouldDelete: false,
                                       GlobalEventPosition: new GlobalEventPosition(11),
                                       ExpectedEventNumber: null,
                                       StageToken: 2,
                                       PersistenceStatus: ProjectionPersistenceStatus.InFlight),
                               ],
                           },
                           CancellationToken.None);

        outcomes.Count.ShouldBe(2);
        outcomes.Single(x => x.ModelId == validId).Status.ShouldBe(PersistenceItemOutcomeStatus.Persisted);
        outcomes.Single(x => x.ModelId == invalidId).Status.ShouldBe(PersistenceItemOutcomeStatus.Failed);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();

        (await dbContext.TestModels.FindAsync(new object[] { validId, }, CancellationToken.None)).ShouldNotBeNull();
        (await dbContext.TestModels.FindAsync(new object[] { invalidId, }, CancellationToken.None)).ShouldBeNull();
    }

    [Fact]
    public async Task DuplicateInsert_Should_Return_SkippedAsStale()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var modelId = Guid.NewGuid();
        await SeedModel(provider, modelId, "existing", 1, 5);
        var store = CreateStore(provider);

        var outcomes = await store.WriteBatch(
                           new PersistenceWriteBatch<TestModel>
                           {
                               Items =
                               [
                                   new ProjectedStateEnvelope<TestModel>(
                                       new TestModel
                                       {
                                           Id = modelId,
                                           Name = "existing",
                                           EventNumber = 1,
                                           GlobalEventPosition = new GlobalEventPosition(5),
                                           HasStreamProcessingFaulted = false,
                                       },
                                       IsNew: true,
                                       ShouldDelete: false,
                                       GlobalEventPosition: new GlobalEventPosition(5),
                                       ExpectedEventNumber: null,
                                       StageToken: 1,
                                       PersistenceStatus: ProjectionPersistenceStatus.InFlight),
                               ],
                           },
                           CancellationToken.None);

        outcomes.Count.ShouldBe(1);
        outcomes[0].Status.ShouldBe(PersistenceItemOutcomeStatus.SkippedAsStale);
    }
}
