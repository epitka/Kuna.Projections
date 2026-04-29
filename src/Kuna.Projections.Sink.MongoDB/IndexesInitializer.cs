using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class IndexesInitializer<TState> : IProjectionStartupTask
    where TState : class, IModel, new()
{
    private readonly IMongoDatabase database;
    private readonly string modelCollectionName;
    private readonly string checkpointCollectionName;
    private readonly string failureCollectionName;
    private readonly IMongoCollection<ProjectionFailureDocument> failureCollection;

    public IndexesInitializer(ProjectionContext<TState> context)
    {
        MongoModelClassMapRegistry.EnsureInitialized<TState>();
        this.database = context.Database;
        this.modelCollectionName = context.CollectionNamer.GetModelCollectionName<TState>();
        this.checkpointCollectionName = context.CollectionNamer.GetCheckpointCollectionName();
        this.failureCollectionName = context.CollectionNamer.GetFailureCollectionName();
        this.failureCollection = context.Database.GetCollection<ProjectionFailureDocument>(this.failureCollectionName);
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        await this.EnsureCollectionExists(this.modelCollectionName, cancellationToken);
        await this.EnsureCollectionExists(this.checkpointCollectionName, cancellationToken);
        await this.EnsureCollectionExists(this.failureCollectionName, cancellationToken);

        CreateIndexModel<ProjectionFailureDocument> failureModelIndex = new(
            Builders<ProjectionFailureDocument>.IndexKeys
                                               .Ascending(x => x.ModelName)
                                               .Ascending(x => x.ModelId),
            new CreateIndexOptions
            {
                Name = "ux_projection_failure_model_name_model_id",
                Unique = true,
            });

        await this.failureCollection.Indexes.CreateOneAsync(failureModelIndex, cancellationToken: cancellationToken);
    }

    private async Task EnsureCollectionExists(string collectionName, CancellationToken cancellationToken)
    {
        using IAsyncCursor<string> collectionNames = await this.database.ListCollectionNamesAsync(cancellationToken: cancellationToken);
        IReadOnlyCollection<string> existingCollectionNames = await collectionNames.ToListAsync(cancellationToken);
        var collectionExists = existingCollectionNames.Contains(collectionName, StringComparer.Ordinal);

        if (!collectionExists)
        {
            await this.database.CreateCollectionAsync(collectionName, cancellationToken: cancellationToken);
        }
    }
}
