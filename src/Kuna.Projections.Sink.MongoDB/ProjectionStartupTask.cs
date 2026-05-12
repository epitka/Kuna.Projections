using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionStartupTask<TState> : IProjectionStartupTask
    where TState : class, IModel, new()
{
    private readonly IMongoDatabase database;
    private readonly string modelCollectionName;
    private readonly string checkpointCollectionName;
    private readonly string failureCollectionName;

    public ProjectionStartupTask(IMongoDatabase database, ICollectionNamer collectionNamer)
    {
        this.database = database;
        this.modelCollectionName = collectionNamer.GetModelCollectionName<TState>();
        this.checkpointCollectionName = collectionNamer.GetCheckpointCollectionName();
        this.failureCollectionName = collectionNamer.GetFailureCollectionName();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var existingCollectionNames = await this.GetExistingCollectionNames(cancellationToken);

        await this.EnsureCollectionExists(this.modelCollectionName, existingCollectionNames, cancellationToken);
        await this.EnsureCollectionExists(this.checkpointCollectionName, existingCollectionNames, cancellationToken);
        await this.EnsureCollectionExists(this.failureCollectionName, existingCollectionNames, cancellationToken);

        CreateIndexModel<ProjectionFailureDocument> failureModelIndex = new(
            Builders<ProjectionFailureDocument>.IndexKeys
                                               .Ascending(x => x.ModelName)
                                               .Ascending(x => x.ModelId),
            new CreateIndexOptions
            {
                Name = "ux_projection_failure_model_name_model_id",
                Unique = true,
            });

        var failureCollection = this.database.GetCollection<ProjectionFailureDocument>(this.failureCollectionName);
        await failureCollection.Indexes.CreateOneAsync(failureModelIndex, cancellationToken: cancellationToken);
    }

    private async Task<HashSet<string>> GetExistingCollectionNames(CancellationToken cancellationToken)
    {
        using var collectionNames = await this.database.ListCollectionNamesAsync(cancellationToken: cancellationToken);
        var existingCollectionNames = await collectionNames.ToListAsync(cancellationToken);
        return existingCollectionNames.ToHashSet(StringComparer.Ordinal);
    }

    private async Task EnsureCollectionExists(
        string collectionName,
        ISet<string> existingCollectionNames,
        CancellationToken cancellationToken)
    {
        if (!existingCollectionNames.Contains(collectionName))
        {
            await this.database.CreateCollectionAsync(collectionName, cancellationToken: cancellationToken);
            existingCollectionNames.Add(collectionName);
        }
    }
}
