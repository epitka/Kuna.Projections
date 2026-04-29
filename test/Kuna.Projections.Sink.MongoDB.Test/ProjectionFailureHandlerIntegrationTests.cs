using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class ProjectionFailureHandlerIntegrationTests : MongoDbIntegrationTestBase
{
    public ProjectionFailureHandlerIntegrationTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task Handle_Should_Mark_Model_As_Faulted_And_Persist_Failure()
    {
        Guid modelId = Guid.NewGuid();
        ProjectionFailure failure = new(
            modelId: modelId,
            eventNumber: 7,
            streamPosition: new GlobalEventPosition(42),
            failureCreatedOn: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            exception: "boom",
            failureType: nameof(FailureType.Persistence),
            modelName: ProjectionModelName.For<TestModel>());

        await using ServiceProvider provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "fault-target", 7, 42);
        IProjectionFailureHandler<TestModel> handler = provider.GetRequiredService<IProjectionFailureHandler<TestModel>>();

        await handler.Handle(failure, CancellationToken.None);

        var modelDocument = await this.GetModelDocument(provider, modelId);
        var failureDocument = await this.GetFailureDocument(provider, modelId);

        modelDocument.ShouldNotBeNull();
        modelDocument["HasStreamProcessingFaulted"].AsBoolean.ShouldBeTrue();

        failureDocument.ShouldNotBeNull();
        failureDocument["ModelName"].AsString.ShouldBe(ProjectionModelName.For<TestModel>());
        failureDocument["ModelId"].AsString.ShouldBe(modelId.ToString("D"));
        failureDocument["EventNumber"].AsInt64.ShouldBe(7);
        failureDocument["GlobalEventPosition"].AsString.ShouldBe("42");
        failureDocument["Exception"].AsString.ShouldBe("boom");
        failureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.Persistence));
    }
}
