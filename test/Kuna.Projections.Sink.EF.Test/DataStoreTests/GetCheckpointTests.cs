using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class GetCheckpointTests : DataStoreIntegrationTestBase
{
    private const string InstanceId = "test-instance";

    public GetCheckpointTests(PostgresSqlContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task ExistingCheckpoint_Should_Be_Returned()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var position = new GlobalEventPosition(long.MaxValue.ToString());

        await store.PersistCheckpoint(
            new CheckPoint
            {
                ModelName = ProjectionModelName.For<TestModel>(),
                InstanceId = InstanceId,
                GlobalEventPosition = position,
            },
            CancellationToken.None);

        var checkpoint = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), InstanceId, CancellationToken.None);

        checkpoint.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        checkpoint.InstanceId.ShouldBe(InstanceId);
        checkpoint.GlobalEventPosition.ShouldBe(position);
    }

    [Fact]
    public async Task MissingCheckpoint_Should_Return_Default_Start_Position()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);

        var checkpoint = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), InstanceId, CancellationToken.None);

        checkpoint.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        checkpoint.InstanceId.ShouldBe(InstanceId);
        checkpoint.GlobalEventPosition.ShouldBe(new GlobalEventPosition(string.Empty));
    }
}
