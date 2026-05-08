using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class CollectionNamingIntegrationTests : MongoDbIntegrationTestBase
{
    public CollectionNamingIntegrationTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task RunStartupTasks_Should_Use_Custom_Model_Collection_Name()
    {
        await using var provider = this.CreateProvider(
            options =>
            {
                options.SetModelCollectionName<Items.TestModel>("read_models_custom_orders");
            });

        await this.RunStartupTasks(provider);

        var collectionNames = await this.GetCollectionNames();

        collectionNames.ShouldContain("read_models_custom_orders");
        collectionNames.ShouldNotContain("projection_test_model");
    }

    [Fact]
    public async Task RunStartupTasks_Should_Use_Custom_Collection_Namer_Implementation()
    {
        ServiceCollection services = new();

        services.AddMongoProjectionsDataStore<Items.TestModel>(
            SettingsSectionName,
            this.Fixture.ConnectionString,
            this.DatabaseName,
            options =>
            {
            },
            options => new FixedCollectionNamer(
                modelCollectionName: "custom_models",
                checkpointCollectionName: "custom_checkpoints"));

        await using var provider = services.BuildServiceProvider();

        await this.RunStartupTasks(provider);

        var collectionNames = await this.GetCollectionNames();

        collectionNames.ShouldContain("custom_models");
        collectionNames.ShouldContain("custom_checkpoints");
        collectionNames.ShouldNotContain("projection_test_model");
        collectionNames.ShouldNotContain("projection_checkpoints");
        collectionNames.ShouldNotContain("projection_failures");
    }

    private sealed class FixedCollectionNamer : ICollectionNamer
    {
        private readonly string modelCollectionName;
        private readonly string checkpointCollectionName;

        public FixedCollectionNamer(
            string modelCollectionName,
            string checkpointCollectionName)
        {
            this.modelCollectionName = modelCollectionName;
            this.checkpointCollectionName = checkpointCollectionName;
        }

        public string GetModelCollectionName<TState>()
            where TState : class, Kuna.Projections.Abstractions.Models.IModel, new()
        {
            return this.modelCollectionName;
        }

        public string GetCheckpointCollectionName()
        {
            return this.checkpointCollectionName;
        }
    }
}
