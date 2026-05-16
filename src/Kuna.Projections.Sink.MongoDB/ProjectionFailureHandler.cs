using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureHandler<TState> : IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
{
    private const int MaxExceptionLength = 500;

    private readonly IMongoDatabase database;
    private readonly IMongoCollection<TState> modelCollection;
    private readonly IMongoCollection<ProjectionFailureDocument> failureCollection;

    public ProjectionFailureHandler(IMongoDatabase database, ICollectionNamer collectionNamer)
    {
        this.database = database;
        this.modelCollection = database.GetCollection<TState>(collectionNamer.GetModelCollectionName<TState>());
        this.failureCollection = database.GetCollection<ProjectionFailureDocument>(collectionNamer.GetFailureCollectionName());
    }

    public async Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        using var session = await this.database.Client.StartSessionAsync(cancellationToken: cancellationToken);
        session.StartTransaction();

        try
        {
            var failureId = $"{failure.ModelName}:{failure.InstanceId}:{failure.ModelId:D}";
            var failureFilter = Builders<ProjectionFailureDocument>.Filter.Eq(x => x.Id, failureId);
            var failureUpdate = Builders<ProjectionFailureDocument>.Update
                                                                  .SetOnInsert(x => x.Id, failureId)
                                                                  .SetOnInsert(x => x.ModelName, failure.ModelName)
                                                                  .SetOnInsert(x => x.InstanceId, failure.InstanceId)
                                                                  .SetOnInsert(x => x.ModelId, failure.ModelId.ToString("D"))
                                                                  .SetOnInsert(x => x.EventNumber, failure.EventNumber)
                                                                  .SetOnInsert(x => x.GlobalEventPosition, failure.GlobalEventPosition.ToString())
                                                                  .SetOnInsert(x => x.FailureCreatedOn, failure.FailureCreatedOn)
                                                                  .SetOnInsert(x => x.Exception, Truncate(failure.Exception))
                                                                  .SetOnInsert(x => x.FailureType, failure.FailureType);

            await this.failureCollection.UpdateOneAsync(
                session,
                failureFilter,
                failureUpdate,
                new UpdateOptions
                {
                    IsUpsert = true,
                },
                cancellationToken);

            var modelFilter = Builders<TState>.Filter.Eq(x => x.Id, failure.ModelId);
            var modelUpdate = Builders<TState>.Update.Set(x => x.HasStreamProcessingFaulted, true);

            await this.modelCollection.UpdateOneAsync(session, modelFilter, modelUpdate, cancellationToken: cancellationToken);

            await session.CommitTransactionAsync(cancellationToken);
        }
        catch
        {
            await session.AbortTransactionAsync(cancellationToken);
            throw;
        }
    }

    private static string Truncate(string value)
    {
        return value.Length <= MaxExceptionLength
                   ? value
                   : value[..MaxExceptionLength];
    }
}
