using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class PersistCheckpointTests : MongoDbIntegrationTestBase
{
    public PersistCheckpointTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task PersistCheckpoint_Should_Roundtrip_Global_Event_Position()
    {
        await using var provider = this.CreateProvider();
        var store = provider.GetRequiredKeyedService<ICheckpointStore>(GetRegistrationKey<TestModel>());
        CheckPoint checkPoint = new()
        {
            ModelName = ProjectionModelName.For<TestModel>(),
            InstanceId = SettingsSectionName,
            GlobalEventPosition = new GlobalEventPosition(123L),
        };

        await store.PersistCheckpoint(checkPoint, CancellationToken.None);

        var result = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), SettingsSectionName, CancellationToken.None);

        result.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        result.InstanceId.ShouldBe(SettingsSectionName);
        result.GlobalEventPosition.ShouldBe(new GlobalEventPosition("123"));
    }

    [Fact]
    public async Task GetCheckpoint_Should_Return_Default_Checkpoint_When_Not_Persisted()
    {
        await using var provider = this.CreateProvider();
        var store = provider.GetRequiredKeyedService<ICheckpointStore>(GetRegistrationKey<TestModel>());

        var result = await store.GetCheckpoint(ProjectionModelName.For<TestModel>(), SettingsSectionName, CancellationToken.None);

        result.ModelName.ShouldBe(ProjectionModelName.For<TestModel>());
        result.InstanceId.ShouldBe(SettingsSectionName);
        result.GlobalEventPosition.ShouldBe(new GlobalEventPosition(string.Empty));
    }
}
