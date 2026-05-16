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
        var failureIndexes = await this.GetIndexes("projection_failures");

        modelIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        checkpointIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        failureIndexes.ShouldContain(index => index["name"].AsString == "_id_");
        failureIndexes.ShouldContain(index => index["name"].AsString == "ux_projection_failure_model_name_instance_id_model_id");
    }

    [Fact]
    public async Task RunStartupTasks_Should_Be_Idempotent_When_Run_Multiple_Times()
    {
        await using var provider = this.CreateProvider();

        await this.RunStartupTasks(provider);
        await this.RunStartupTasks(provider);

        var failureIndexes = await this.GetIndexes("projection_failures");

        failureIndexes.Count(index => index["name"].AsString == "ux_projection_failure_model_name_instance_id_model_id")
                      .ShouldBe(1);
    }

    [Fact]
    public async Task RunStartupTasks_Should_Be_Safe_When_Two_Providers_Initialize_The_Same_Database()
    {
        await using var firstProvider = this.CreateProvider("orders-v1");
        await using var secondProvider = this.CreateProvider("orders-v2");

        await Task.WhenAll(
            this.RunStartupTasks(firstProvider),
            this.RunStartupTasks(secondProvider));

        var failureIndexes = await this.GetIndexes("projection_failures");

        failureIndexes.Count(index => index["name"].AsString == "ux_projection_failure_model_name_instance_id_model_id")
                      .ShouldBe(1);
    }
}
