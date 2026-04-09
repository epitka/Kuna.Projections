using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

public abstract class DataStoreIntegrationTestBase
{
    protected DataStoreIntegrationTestBase(PostgresSqlContainerFixture fixture)
    {
        this.Fixture = fixture;
        PostgresSqlTestHelper.ResetDatabase(fixture);
    }

    protected PostgresSqlContainerFixture Fixture { get; }

    protected static DataStore<TestModel, TestProjectionDbContext> CreateStore(ServiceProvider provider)
    {
        var failureLogger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<TestModel, TestProjectionDbContext>>>();
        var failureHandler = new ProjectionFailureHandler<TestModel, TestProjectionDbContext>(provider, failureLogger);
        var storeLogger = provider.GetRequiredService<ILogger<DataStore<TestModel, TestProjectionDbContext>>>();
        return new DataStore<TestModel, TestProjectionDbContext>(provider, failureHandler, storeLogger);
    }

    protected static async Task SeedModel(
        ServiceProvider provider,
        Guid modelId,
        string name,
        long eventNumber,
        ulong projectionStreamPosition)
    {
        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();

        dbContext.TestModels.Add(
            new TestModel
            {
                Id = modelId,
                Name = name,
                EventNumber = eventNumber,
                GlobalEventPosition = new GlobalEventPosition(projectionStreamPosition),
                HasStreamProcessingFaulted = false,
            });

        await dbContext.SaveChangesAsync(CancellationToken.None);
    }
}
