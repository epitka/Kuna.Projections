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
}
