using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB.Test;

public abstract class MongoDbIntegrationTestBase
{
    protected const string SettingsSectionName = "MongoProjection";

    protected MongoDbIntegrationTestBase(MongoDbContainerFixture fixture)
    {
        this.Fixture = fixture;
        this.DatabaseName = $"kuna-projections-test-{Guid.NewGuid():N}";
    }

    protected MongoDbContainerFixture Fixture { get; }

    protected string DatabaseName { get; }

    protected static string GetRegistrationKey<TState>()
        where TState : class, Kuna.Projections.Abstractions.Models.IModel, new()
    {
        return ProjectionRegistration.GetKey<TState>(SettingsSectionName);
    }

    protected ServiceProvider CreateProvider()
    {
        return this.CreateProvider(
            _ =>
            {
            });
    }

    protected ServiceProvider CreateProvider(Action<Kuna.Projections.Sink.MongoDB.ProjectionOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddMongoProjectionsDataStore<TestModel>(
            SettingsSectionName,
            this.Fixture.ConnectionString,
            this.DatabaseName,
            options =>
            {
                options.CollectionPrefix = "projection";
                configure(options);
            });

        return services.BuildServiceProvider();
    }

    protected async Task RunStartupTasks(ServiceProvider provider)
    {
        foreach (var startupTask in provider.GetServices<IProjectionStartupTask>())
        {
            await startupTask.RunAsync(CancellationToken.None);
        }
    }

    protected async Task SeedModel(
        ServiceProvider provider,
        Guid modelId,
        string name,
        long eventNumber,
        ulong globalEventPosition,
        bool hasStreamProcessingFaulted = false)
    {
        var database = this.CreateDatabase();
        var collection = database.GetCollection<BsonDocument>("projection_test_model");

        BsonDocument document =
        [
            new BsonElement("_id", modelId.ToString("D")),
            new BsonElement("Name", name),
            new BsonElement("EventNumber", eventNumber),
            new BsonElement("GlobalEventPosition", globalEventPosition.ToString()),
            new BsonElement("HasStreamProcessingFaulted", hasStreamProcessingFaulted),
        ];

        await collection.InsertOneAsync(document);
    }

    protected async Task<BsonDocument?> GetModelDocument(ServiceProvider provider, Guid modelId)
    {
        var database = this.CreateDatabase();
        var collection = database.GetCollection<BsonDocument>("projection_test_model");
        return await collection.Find(x => x["_id"] == modelId.ToString("D")).SingleOrDefaultAsync();
    }

    protected async Task<BsonDocument?> GetFailureDocument(ServiceProvider provider, Guid modelId)
    {
        var failureId = $"{typeof(TestModel).FullName}:{modelId:D}";
        var database = this.CreateDatabase();
        var collection = database.GetCollection<BsonDocument>("projection_failures");
        return await collection.Find(x => x["_id"] == failureId).SingleOrDefaultAsync();
    }

    protected async Task<long> GetModelDocumentCount(ServiceProvider provider, Guid modelId)
    {
        var database = this.CreateDatabase();
        var collection = database.GetCollection<BsonDocument>("projection_test_model");
        return await collection.CountDocumentsAsync(x => x["_id"] == modelId.ToString("D"));
    }

    protected async Task<IReadOnlyList<BsonDocument>> GetIndexes(string collectionName)
    {
        var database = this.CreateDatabase();
        IAsyncCursor<BsonDocument> cursor = await database.GetCollection<BsonDocument>(collectionName).Indexes.ListAsync();
        return await cursor.ToListAsync();
    }

    protected async Task<IReadOnlyList<string>> GetCollectionNames()
    {
        var database = this.CreateDatabase();
        IAsyncCursor<string> cursor = await database.ListCollectionNamesAsync();
        return await cursor.ToListAsync();
    }

    private IMongoDatabase CreateDatabase()
    {
        return new MongoClient(this.Fixture.ConnectionString).GetDatabase(this.DatabaseName);
    }
}
