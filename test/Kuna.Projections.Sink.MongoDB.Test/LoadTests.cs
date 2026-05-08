using Kuna.Projections.Abstractions.Models;
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
        await using var provider = this.CreateProvider();
        var store = provider.GetRequiredKeyedService<IModelStateStore<TestModel>>(GetRegistrationKey<TestModel>());

        var result = await store.Load(Guid.NewGuid(), CancellationToken.None);

        result.ShouldBeNull();
    }

    [Fact]
    public async Task Load_Should_Return_Persisted_Model()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "alpha", 42, 101);
        var store = provider.GetRequiredKeyedService<IModelStateStore<TestModel>>(GetRegistrationKey<TestModel>());

        var result = await store.Load(modelId, CancellationToken.None);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(modelId);
        result.Name.ShouldBe("alpha");
        result.EventNumber.ShouldBe(42);
        result.GlobalEventPosition.ShouldBe(new Kuna.Projections.Abstractions.Models.GlobalEventPosition("101"));
        result.HasStreamProcessingFaulted.ShouldBeFalse();
    }
}
