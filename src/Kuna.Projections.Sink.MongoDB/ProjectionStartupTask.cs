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
        await this.EnsureCollectionExists(this.modelCollectionName, cancellationToken);
        await this.EnsureCollectionExists(this.checkpointCollectionName, cancellationToken);
        await this.EnsureCollectionExists(this.failureCollectionName, cancellationToken);

        CreateIndexModel<ProjectionFailureDocument> failureModelIndex = new(
            Builders<ProjectionFailureDocument>.IndexKeys
                                               .Ascending(x => x.ModelName)
                                               .Ascending(x => x.InstanceId)
                                               .Ascending(x => x.ModelId),
            new CreateIndexOptions
            {
                Name = "ux_projection_failure_model_name_instance_id_model_id",
                Unique = true,
            });

        var failureCollection = this.database.GetCollection<ProjectionFailureDocument>(this.failureCollectionName);
        await failureCollection.Indexes.CreateOneAsync(failureModelIndex, cancellationToken: cancellationToken);
    }

    private async Task EnsureCollectionExists(
        string collectionName,
        CancellationToken cancellationToken)
    {
        try
        {
            await this.database.CreateCollectionAsync(collectionName, cancellationToken: cancellationToken);
        }
        catch (MongoCommandException ex) when (ex.Code == 48 || string.Equals(ex.CodeName, "NamespaceExists", StringComparison.Ordinal))
        {
        }
    }
}
