using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class LoadTests : MongoDbIntegrationTestBase
{
    public LoadTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task Load_Should_Return_Null_For_Unknown_Id()
    {
        await using ServiceProvider provider = this.CreateProvider();
        IModelStateStore<TestModel> store = provider.GetRequiredService<IModelStateStore<TestModel>>();

        TestModel? result = await store.Load(Guid.NewGuid(), CancellationToken.None);

        result.ShouldBeNull();
    }

    [Fact]
    public async Task Load_Should_Return_Persisted_Model()
    {
        Guid modelId = Guid.NewGuid();

        await using ServiceProvider provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "alpha", 42, 101);
        IModelStateStore<TestModel> store = provider.GetRequiredService<IModelStateStore<TestModel>>();

        TestModel? result = await store.Load(modelId, CancellationToken.None);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(modelId);
        result.Name.ShouldBe("alpha");
        result.EventNumber.ShouldBe(42);
        result.GlobalEventPosition.Value.ShouldBe(101UL);
        result.HasStreamProcessingFaulted.ShouldBeFalse();
    }
}
