using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

[Collection(PostgresSqlCollection.Name)]
public class ProjectionFailureHandlerIntegrationTests
{
    private readonly PostgresSqlContainerFixture fixture;

    public ProjectionFailureHandlerIntegrationTests(PostgresSqlContainerFixture fixture)
    {
        this.fixture = fixture;
        PostgresSqlTestHelper.ResetDatabase(fixture);
    }

    [Fact]
    public async Task Handle_Should_Persist_Failure_And_Mark_Model_As_Faulted()
    {
        var modelId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.fixture);
        await SeedModel(provider, modelId);

        var logger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<TestModel, TestProjectionDbContext>>>();
        var handler = new ProjectionFailureHandler<TestModel, TestProjectionDbContext>(provider, logger);
        var failure = CreateFailure(modelId, "first");

        await handler.Handle(failure, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedFailure = await dbContext.ProjectionFailures.FindAsync(
                                   new object[] { failure.ModelName, modelId, },
                                   CancellationToken.None);

        var persistedModel = await dbContext.TestModels.FindAsync(new object[] { modelId, }, CancellationToken.None);

        persistedFailure.ShouldNotBeNull();
        persistedFailure.Exception.ShouldBe("first");
        persistedModel.ShouldNotBeNull();
        persistedModel.HasStreamProcessingFaulted.ShouldBeTrue();
    }

    [Fact]
    public async Task Handle_Should_Append_And_Truncate_On_Duplicate_Failure()
    {
        var modelId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.fixture);
        await SeedModel(provider, modelId);

        var logger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<TestModel, TestProjectionDbContext>>>();
        var handler = new ProjectionFailureHandler<TestModel, TestProjectionDbContext>(provider, logger);

        await handler.Handle(CreateFailure(modelId, new string('a', 3950)), CancellationToken.None);
        await handler.Handle(CreateFailure(modelId, new string('b', 200)), CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var failures = await dbContext.ProjectionFailures
                                      .Where(x => x.ModelId == modelId && x.ModelName == ProjectionModelName.For<TestModel>())
                                      .ToListAsync(CancellationToken.None);

        failures.Count.ShouldBe(1);
        failures[0].Exception.Length.ShouldBeLessThanOrEqualTo(3999);
        failures[0].Exception.ShouldContain("Additional_Failure:");
    }

    private static ProjectionFailure CreateFailure(Guid modelId, string exception)
    {
        return new ProjectionFailure(
            modelId: modelId,
            eventNumber: 1,
            streamPosition: new GlobalEventPosition(10),
            failureCreatedOn: DateTime.UtcNow,
            exception: exception,
            failureType: nameof(FailureType.EventProcessing),
            modelName: ProjectionModelName.For<TestModel>());
    }

    private static async Task SeedModel(IServiceProvider provider, Guid modelId)
    {
        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();

        dbContext.TestModels.Add(
            new TestModel
            {
                Id = modelId,
                Name = "seed",
                EventNumber = 1,
                GlobalEventPosition = new GlobalEventPosition(1),
                HasStreamProcessingFaulted = false,
            });

        await dbContext.SaveChangesAsync(CancellationToken.None);
    }
}
