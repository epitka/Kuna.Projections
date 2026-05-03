using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointStore<TState> : ICheckpointStore
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<ProjectionCheckpointDocument> collection;

    public ProjectionCheckpointStore(ProjectionContext<TState> context)
    {
        this.collection = context.Database.GetCollection<ProjectionCheckpointDocument>(context.CollectionNamer.GetCheckpointCollectionName());
    }

    public async Task<CheckPoint> GetCheckpoint(string modelName, CancellationToken cancellationToken)
    {
        var document = await this.collection
                                 .Find(x => x.ModelName == modelName)
                                 .SingleOrDefaultAsync(cancellationToken);

        if (document is null)
        {
            return new CheckPoint
            {
                ModelName = modelName,
                GlobalEventPosition = new GlobalEventPosition(0),
            };
        }

        return new CheckPoint
        {
            ModelName = document.ModelName,
            GlobalEventPosition = GlobalEventPosition.From(document.GlobalEventPosition),
        };
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        ProjectionCheckpointDocument document = new()
        {
            ModelName = checkPoint.ModelName,
            GlobalEventPosition = checkPoint.GlobalEventPosition.ToString(),
        };

        return this.collection.ReplaceOneAsync(
            x => x.ModelName == checkPoint.ModelName,
            document,
            new ReplaceOptions
            {
                IsUpsert = true,
            },
            cancellationToken);
    }
}
