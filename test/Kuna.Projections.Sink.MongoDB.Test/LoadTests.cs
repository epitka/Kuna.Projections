using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
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
        await this.SeedModel(provider, modelId, "alpha", 42, "101");
        var store = provider.GetRequiredKeyedService<IModelStateStore<TestModel>>(GetRegistrationKey<TestModel>());

        var result = await store.Load(modelId, CancellationToken.None);

        result.ShouldNotBeNull();
        result.Id.ShouldBe(modelId);
        result.Name.ShouldBe("alpha");
        result.EventNumber.ShouldBe(42);
        result.GlobalEventPosition.ShouldBe(new Kuna.Projections.Abstractions.Models.GlobalEventPosition("101"));
        result.HasStreamProcessingFaulted.ShouldBeFalse();
    }

    [Fact]
    public async Task Load_Should_Deserialize_Guid_Fields_In_Model_Graph()
    {
        var modelId = Guid.NewGuid();
        var externalId = Guid.NewGuid();
        var childCustomerId = Guid.NewGuid();
        var refundId = Guid.NewGuid();
        var cancellationToken = TestContext.Current.CancellationToken;

        await using var provider = this.CreateProvider();
        var database = new MongoClient(this.Fixture.ConnectionString).GetDatabase(this.DatabaseName);
        var collection = database.GetCollection<BsonDocument>("projection_test_model");

        BsonDocument document =
        [
            new BsonElement("_id", modelId.ToString("D")),
            new BsonElement("Name", "alpha"),
            new BsonElement("ExternalId", externalId.ToString("D")),
            new BsonElement("Child", new BsonDocument("CustomerId", childCustomerId.ToString("D"))),
            new BsonElement("Items", new BsonArray { new BsonDocument("RefundId", refundId.ToString("D")), }),
            new BsonElement("EventNumber", 42),
            new BsonElement("GlobalEventPosition", "101"),
            new BsonElement("HasStreamProcessingFaulted", false),
        ];

        await collection.InsertOneAsync(document, options: null, cancellationToken);

        var store = provider.GetRequiredKeyedService<IModelStateStore<TestModel>>(GetRegistrationKey<TestModel>());
        var result = await store.Load(modelId, cancellationToken);

        result.ShouldNotBeNull();
        result.ExternalId.ShouldBe(externalId);
        result.Child.ShouldNotBeNull();
        result.Child.CustomerId.ShouldBe(childCustomerId);
        result.Items.Count.ShouldBe(1);
        result.Items[0].RefundId.ShouldBe(refundId);
    }
}
