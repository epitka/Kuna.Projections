using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureHandler<TState> : IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
{
    private const int MaxExceptionLength = 500;

    private readonly IMongoCollection<TState> modelCollection;
    private readonly IMongoCollection<ProjectionFailureDocument> failureCollection;

    public ProjectionFailureHandler(ProjectionContext<TState> context)
    {
        MongoModelClassMapRegistry.EnsureInitialized<TState>();
        this.modelCollection = context.Database.GetCollection<TState>(context.CollectionNamer.GetModelCollectionName<TState>());
        this.failureCollection = context.Database.GetCollection<ProjectionFailureDocument>(context.CollectionNamer.GetFailureCollectionName());
    }

    public async Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        var modelFilter = Builders<TState>.Filter.Eq(x => x.Id, failure.ModelId);
        var modelUpdate = Builders<TState>.Update.Set(x => x.HasStreamProcessingFaulted, true);

        await this.modelCollection.UpdateOneAsync(modelFilter, modelUpdate, cancellationToken: cancellationToken);

        ProjectionFailureDocument document = new()
        {
            Id = $"{failure.ModelName}:{MongoGuid.Format(failure.ModelId)}",
            ModelName = failure.ModelName,
            ModelId = MongoGuid.Format(failure.ModelId),
            EventNumber = failure.EventNumber,
            GlobalEventPosition = GlobalEventPositionConverter.Format(failure.GlobalEventPosition),
            FailureCreatedOn = failure.FailureCreatedOn,
            Exception = Truncate(failure.Exception),
            FailureType = failure.FailureType,
        };

        await this.failureCollection.ReplaceOneAsync(
            x => x.Id == document.Id,
            document,
            new ReplaceOptions
            {
                IsUpsert = true,
            },
            cancellationToken);
    }

    private static string Truncate(string value)
    {
        return value.Length <= MaxExceptionLength
                   ? value
                   : value[..MaxExceptionLength];
    }
}
