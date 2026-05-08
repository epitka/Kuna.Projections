using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class IndexesInitializerIntegrationTests : MongoDbIntegrationTestBase
{
    public IndexesInitializerIntegrationTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task RunStartupTasks_Should_Create_Required_Indexes()
    {
        await using var provider = this.CreateProvider();

        await this.RunStartupTasks(provider);

        var modelIndexes = await this.GetIndexes("projection_test_model");
        var checkpointIndexes = await this.GetIndexes("projection_checkpoints");

        modelIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        checkpointIndexes.ShouldContain(index => index["name"].AsString == "_id_");
    }
}
