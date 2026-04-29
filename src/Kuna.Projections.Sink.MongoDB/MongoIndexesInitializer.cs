using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class MongoIndexesInitializer<TState> : IProjectionStartupTask
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<TState> modelCollection;
    private readonly IMongoCollection<ProjectionCheckpointDocument> checkpointCollection;
    private readonly IMongoCollection<ProjectionFailureDocument> failureCollection;

    public MongoIndexesInitializer(IMongoDatabase database, CollectionNamer collectionNamer)
    {
        MongoModelClassMapRegistry.EnsureInitialized<TState>();
        this.modelCollection = database.GetCollection<TState>(collectionNamer.GetModelCollectionName<TState>());
        this.checkpointCollection = database.GetCollection<ProjectionCheckpointDocument>(collectionNamer.GetCheckpointCollectionName());
        this.failureCollection = database.GetCollection<ProjectionFailureDocument>(collectionNamer.GetFailureCollectionName());
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        CreateIndexModel<TState> modelIdIndex = new(
            Builders<TState>.IndexKeys.Ascending(x => x.Id),
            new CreateIndexOptions
            {
                Name = "ux_projection_model_id",
                Unique = true,
            });

        CreateIndexModel<ProjectionCheckpointDocument> checkpointModelNameIndex = new(
            Builders<ProjectionCheckpointDocument>.IndexKeys.Ascending(x => x.ModelName),
            new CreateIndexOptions
            {
                Name = "ux_projection_checkpoint_model_name",
                Unique = true,
            });

        CreateIndexModel<ProjectionFailureDocument> failureModelIndex = new(
            Builders<ProjectionFailureDocument>.IndexKeys
                                               .Ascending(x => x.ModelName)
                                               .Ascending(x => x.ModelId),
            new CreateIndexOptions
            {
                Name = "ux_projection_failure_model_name_model_id",
                Unique = true,
            });

        await this.modelCollection.Indexes.CreateOneAsync(modelIdIndex, cancellationToken: cancellationToken);
        await this.checkpointCollection.Indexes.CreateOneAsync(checkpointModelNameIndex, cancellationToken: cancellationToken);
        await this.failureCollection.Indexes.CreateOneAsync(failureModelIndex, cancellationToken: cancellationToken);
    }
}
