using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureHandler<TState> : IProjectionFailureHandler<TState>
    where TState : class, IModel, new()
{
    private const int MaxExceptionLength = 500;

    private readonly IMongoCollection<BsonDocument> modelCollection;

    public ProjectionFailureHandler(ProjectionContext<TState> context)
    {
        this.modelCollection = context.Database.GetCollection<BsonDocument>(context.CollectionNamer.GetModelCollectionName<TState>());
    }

    public async Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        var modelId = failure.ModelId.ToString("D");
        var existingDocument = await this.modelCollection
                                         .Find(Builders<BsonDocument>.Filter.Eq("_id", modelId))
                                         .FirstOrDefaultAsync(cancellationToken);

        if (existingDocument is null)
        {
            await this.InsertStubDocument(modelId, failure, cancellationToken);
            return;
        }

        await this.UpdateExistingDocument(existingDocument, modelId, failure, cancellationToken);
    }

    private static BsonDocument CreateProjectionFailureDocument(ProjectionFailure failure)
    {
        return new BsonDocument
        {
            { nameof(ProjectionFailure.EventNumber), failure.EventNumber },
            { nameof(ProjectionFailure.GlobalEventPosition), failure.GlobalEventPosition.ToString() },
            { nameof(ProjectionFailure.FailureCreatedOn), failure.FailureCreatedOn },
            { nameof(ProjectionFailure.Exception), Truncate(failure.Exception) },
            { nameof(ProjectionFailure.FailureType), failure.FailureType },
        };
    }

    private static string Truncate(string value)
    {
        return value.Length <= MaxExceptionLength
                   ? value
                   : value[..MaxExceptionLength];
    }

    private async Task InsertStubDocument(string modelId, ProjectionFailure failure, CancellationToken cancellationToken)
    {
        try
        {
            var document = new BsonDocument
            {
                { "_id", modelId },
                { nameof(IModel.HasStreamProcessingFaulted), true },
                { nameof(IModel.ProjectionFailure), CreateProjectionFailureDocument(failure) },
            };

            await this.modelCollection.InsertOneAsync(document, cancellationToken: cancellationToken);
        }
        catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
        {
            await this.UpdateExistingDocument(
                existingDocument: null,
                modelId,
                failure,
                cancellationToken);
        }
    }

    private async Task UpdateExistingDocument(
        BsonDocument? existingDocument,
        string modelId,
        ProjectionFailure failure,
        CancellationToken cancellationToken)
    {
        var modelFilter = Builders<BsonDocument>.Filter.Eq("_id", modelId);
        var modelUpdate = Builders<BsonDocument>.Update.Set(nameof(IModel.HasStreamProcessingFaulted), true);

        await this.modelCollection.UpdateOneAsync(modelFilter, modelUpdate, cancellationToken: cancellationToken);

        var hasProjectionFailure = existingDocument != null
                                   && existingDocument.TryGetValue(nameof(IModel.ProjectionFailure), out var currentFailure)
                                   && !currentFailure.IsBsonNull;

        if (hasProjectionFailure)
        {
            return;
        }

        var missingFailureFilter = Builders<BsonDocument>.Filter.And(
            modelFilter,
            Builders<BsonDocument>.Filter.Or(
                Builders<BsonDocument>.Filter.Exists(nameof(IModel.ProjectionFailure), false),
                Builders<BsonDocument>.Filter.Eq(nameof(IModel.ProjectionFailure), BsonNull.Value)));

        var missingFailureUpdate = Builders<BsonDocument>.Update.Set(
            nameof(IModel.ProjectionFailure),
            CreateProjectionFailureDocument(failure));

        await this.modelCollection.UpdateOneAsync(
            missingFailureFilter,
            missingFailureUpdate,
            cancellationToken: cancellationToken);
    }
}
