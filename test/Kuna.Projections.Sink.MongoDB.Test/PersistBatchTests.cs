using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class PersistBatchTests : MongoDbIntegrationTestBase
{
    public PersistBatchTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task PersistBatch_Should_Insert_New_Model()
    {
        Guid modelId = Guid.NewGuid();

        await using ServiceProvider provider = this.CreateProvider();
        IModelStateSink<TestModel> sink = provider.GetRequiredService<IModelStateSink<TestModel>>();
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "created",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition(10),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(10),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition(10),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("created");
        document["EventNumber"].AsInt64.ShouldBe(1);
        document["GlobalEventPosition"].AsString.ShouldBe("10");
    }

    [Fact]
    public async Task PersistBatch_Should_Update_Model_When_Expected_Event_Number_Matches()
    {
        Guid modelId = Guid.NewGuid();

        await using ServiceProvider provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "before", 1, 10);
        IModelStateSink<TestModel> sink = provider.GetRequiredService<IModelStateSink<TestModel>>();
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "after",
                        EventNumber = 2,
                        GlobalEventPosition = new GlobalEventPosition(11),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(11),
                    ExpectedEventNumber: 1),
            ],
            GlobalEventPosition = new GlobalEventPosition(11),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("after");
        document["EventNumber"].AsInt64.ShouldBe(2);
        document["GlobalEventPosition"].AsString.ShouldBe("11");
    }

    [Fact]
    public async Task PersistBatch_Should_Delete_Model_When_Expected_Event_Number_Matches()
    {
        Guid modelId = Guid.NewGuid();

        await using ServiceProvider provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "to-delete", 2, 10);
        IModelStateSink<TestModel> sink = provider.GetRequiredService<IModelStateSink<TestModel>>();
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "to-delete",
                        EventNumber = 3,
                        GlobalEventPosition = new GlobalEventPosition(12),
                    },
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition(12),
                    ExpectedEventNumber: 2),
            ],
            GlobalEventPosition = new GlobalEventPosition(12),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldBeNull();
    }
}
