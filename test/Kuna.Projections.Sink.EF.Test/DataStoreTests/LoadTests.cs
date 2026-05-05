using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class LoadTests : DataStoreIntegrationTestBase
{
    public LoadTests(PostgresSqlContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task MissingModel_Should_Return_Null()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);

        var result = await store.Load(Guid.NewGuid(), CancellationToken.None);

        result.ShouldBeNull();
    }

    [Fact]
    public async Task ExistingModel_Should_Be_Returned()
    {
        var modelId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        await SeedModel(provider, modelId, "existing", 3, 30);
        var store = CreateStore(provider);

        var result = await store.Load(modelId, CancellationToken.None);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(modelId);
        result.Name.ShouldBe("existing");
        result.EventNumber.ShouldBe(3);
        result.GlobalEventPosition.ShouldBe(new GlobalEventPosition("30"));
    }
}
