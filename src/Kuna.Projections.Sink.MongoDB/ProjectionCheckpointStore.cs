using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointStore<TState> : ICheckpointStore
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<ProjectionCheckpointDocument> collection;
    private readonly string modelName;

    public ProjectionCheckpointStore(ProjectionContext<TState> context)
    {
        this.collection = context.Database.GetCollection<ProjectionCheckpointDocument>(context.CollectionNamer.GetCheckpointCollectionName());
        this.modelName = ProjectionModelName.For<TState>();
    }

    public async Task<CheckPoint> GetCheckpoint(CancellationToken cancellationToken)
    {
        var document = await this.collection
                                 .Find(x => x.ModelName == this.modelName)
                                 .SingleOrDefaultAsync(cancellationToken);

        if (document is null)
        {
            return new CheckPoint
            {
                ModelName = this.modelName,
                GlobalEventPosition = new GlobalEventPosition(0),
            };
        }

        return new CheckPoint
        {
            ModelName = document.ModelName,
            GlobalEventPosition = GlobalEventPositionConverter.Parse(document.GlobalEventPosition),
        };
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        ProjectionCheckpointDocument document = new()
        {
            ModelName = checkPoint.ModelName,
            GlobalEventPosition = GlobalEventPositionConverter.Format(checkPoint.GlobalEventPosition),
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
