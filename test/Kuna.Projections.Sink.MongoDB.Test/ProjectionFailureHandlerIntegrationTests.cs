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
        var modelId = Guid.NewGuid();
        ProjectionFailure failure = new(
            modelId: modelId,
            eventNumber: 7,
            streamPosition: new GlobalEventPosition(42L),
            failureCreatedOn: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            exception: "boom",
            failureType: nameof(FailureType.Persistence),
            modelName: ProjectionModelName.For<TestModel>(),
            instanceId: SettingsSectionName);

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "fault-target", 7, 42);
        var handler = provider.GetRequiredKeyedService<IProjectionFailureHandler<TestModel>>(GetRegistrationKey<TestModel>());

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

    [Fact]
    public async Task Handle_Should_Replace_Existing_Failure_Text_And_Metadata()
    {
        var modelId = Guid.NewGuid();
        ProjectionFailure firstFailure = new(
            modelId: modelId,
            eventNumber: 7,
            streamPosition: new GlobalEventPosition(42L),
            failureCreatedOn: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            exception: "first",
            failureType: nameof(FailureType.Persistence),
            modelName: ProjectionModelName.For<TestModel>(),
            instanceId: SettingsSectionName);

        ProjectionFailure secondFailure = new(
            modelId: modelId,
            eventNumber: 8,
            streamPosition: new GlobalEventPosition(43L),
            failureCreatedOn: new DateTime(2026, 1, 2, 3, 5, 5, DateTimeKind.Utc),
            exception: "second",
            failureType: nameof(FailureType.EventProcessing),
            modelName: ProjectionModelName.For<TestModel>(),
            instanceId: SettingsSectionName);

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "fault-target", 8, 43);
        var handler = provider.GetRequiredKeyedService<IProjectionFailureHandler<TestModel>>(GetRegistrationKey<TestModel>());

        await handler.Handle(firstFailure, CancellationToken.None);
        await handler.Handle(secondFailure, CancellationToken.None);

        var failureDocument = await this.GetFailureDocument(provider, modelId);

        failureDocument.ShouldNotBeNull();
        failureDocument["EventNumber"].AsInt64.ShouldBe(8);
        failureDocument["GlobalEventPosition"].AsString.ShouldBe("43");
        failureDocument["Exception"].AsString.ShouldBe("second");
        failureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.EventProcessing));
    }

    [Fact]
    public async Task Handle_Should_Truncate_Exception_Text_To_500_Characters()
    {
        var modelId = Guid.NewGuid();
        var exceptionText = new string('x', 600);
        ProjectionFailure failure = new(
            modelId: modelId,
            eventNumber: 7,
            streamPosition: new GlobalEventPosition(42L),
            failureCreatedOn: new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            exception: exceptionText,
            failureType: nameof(FailureType.Persistence),
            modelName: ProjectionModelName.For<TestModel>(),
            instanceId: SettingsSectionName);

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "fault-target", 7, 42);
        var handler = provider.GetRequiredKeyedService<IProjectionFailureHandler<TestModel>>(GetRegistrationKey<TestModel>());

        await handler.Handle(failure, CancellationToken.None);

        var failureDocument = await this.GetFailureDocument(provider, modelId);

        failureDocument.ShouldNotBeNull();
        failureDocument["Exception"].AsString.Length.ShouldBe(500);
    }
}
