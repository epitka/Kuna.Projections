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

    public async Task<CheckPoint> GetCheckpoint(string modelName, string instanceId, CancellationToken cancellationToken)
    {
        var document = await this.collection
                                 .Find(x => x.ModelName == modelName && x.InstanceId == instanceId)
                                 .SingleOrDefaultAsync(cancellationToken);

        if (document is null)
        {
            return new CheckPoint
            {
                ModelName = modelName,
                InstanceId = instanceId,
                GlobalEventPosition = new GlobalEventPosition(string.Empty),
            };
        }

        return new CheckPoint
        {
            ModelName = document.ModelName,
            InstanceId = document.InstanceId,
            GlobalEventPosition = GlobalEventPosition.From(document.GlobalEventPosition),
        };
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        ProjectionCheckpointDocument document = new()
        {
            Id = GetId(checkPoint.ModelName, checkPoint.InstanceId),
            ModelName = checkPoint.ModelName,
            InstanceId = checkPoint.InstanceId,
            GlobalEventPosition = checkPoint.GlobalEventPosition.ToString(),
        };

        return this.collection.ReplaceOneAsync(
            x => x.Id == document.Id,
            document,
            new ReplaceOptions
            {
                IsUpsert = true,
            },
            cancellationToken);
    }

    private static string GetId(string modelName, string instanceId)
    {
        return $"{modelName}:{instanceId}";
    }
}
