using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class MongoIndexesInitializerIntegrationTests : MongoDbIntegrationTestBase
{
    public MongoIndexesInitializerIntegrationTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task RunStartupTasks_Should_Create_Required_Indexes()
    {
        await using ServiceProvider provider = this.CreateProvider();

        await this.RunStartupTasks(provider);

        var modelIndexes = await this.GetIndexes("projection_test_model");
        var checkpointIndexes = await this.GetIndexes("projection_checkpoints");
        var failureIndexes = await this.GetIndexes("projection_failures");

        modelIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        checkpointIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        failureIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        failureIndexes.ShouldContain(index => index["name"].AsString == "ux_projection_failure_model_name_model_id");
    }
}
