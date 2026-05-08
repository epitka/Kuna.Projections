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

    public ProjectionStartupTask(ProjectionContext<TState> context)
    {
        this.database = context.Database;

        this.modelCollectionName = context.CollectionNamer.GetModelCollectionName<TState>();
        this.checkpointCollectionName = context.CollectionNamer.GetCheckpointCollectionName();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var existingCollectionNames = await this.GetExistingCollectionNames(cancellationToken);

        await this.EnsureCollectionExists(this.modelCollectionName, existingCollectionNames, cancellationToken);
        await this.EnsureCollectionExists(this.checkpointCollectionName, existingCollectionNames, cancellationToken);
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
