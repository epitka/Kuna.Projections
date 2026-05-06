using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class PersistCheckpointTests : DataStoreIntegrationTestBase
{
    private const string InstanceId = "test-instance";

    public PersistCheckpointTests(PostgresSqlContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task MissingCheckpoint_Should_Create_And_ExistingCheckpoint_Should_Update()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);

        await store.PersistCheckpoint(
            new CheckPoint
            {
                ModelName = ProjectionModelName.For<TestModel>(),
                InstanceId = InstanceId,
                GlobalEventPosition = new GlobalEventPosition("10"),
            },
            CancellationToken.None);

        await store.PersistCheckpoint(
            new CheckPoint
            {
                ModelName = ProjectionModelName.For<TestModel>(),
                InstanceId = InstanceId,
                GlobalEventPosition = new GlobalEventPosition("25"),
            },
            CancellationToken.None);

        var checkpoint = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), InstanceId, CancellationToken.None);

        checkpoint.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        checkpoint.InstanceId.ShouldBe(InstanceId);
        checkpoint.GlobalEventPosition.ShouldBe(new GlobalEventPosition("25"));
    }
}
