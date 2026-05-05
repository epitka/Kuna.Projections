using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class GetCheckpointTests : DataStoreIntegrationTestBase
{
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
                GlobalEventPosition = position,
            },
            CancellationToken.None);

        var checkpoint = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), CancellationToken.None);

        checkpoint.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        checkpoint.GlobalEventPosition.ShouldBe(position);
    }

    [Fact]
    public async Task MissingCheckpoint_Should_Return_Default_Start_Position()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);

        var checkpoint = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), CancellationToken.None);

        checkpoint.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        checkpoint.GlobalEventPosition.ShouldBe(new GlobalEventPosition(string.Empty));
    }
}
