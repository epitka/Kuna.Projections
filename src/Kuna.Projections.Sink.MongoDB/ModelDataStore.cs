using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ModelDataStore<TState> : IModelStateSink<TState>, IModelStateStore<TState>
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<TState> collection;
    private readonly IProjectionFailureHandler<TState> failureHandler;
    private readonly string modelName;

    public ModelDataStore(
        MongoProjectionContext<TState> context,
        IProjectionFailureHandler<TState> failureHandler)
    {
        MongoModelClassMapRegistry.EnsureInitialized<TState>();
        this.collection = context.Database.GetCollection<TState>(context.CollectionNamer.GetModelCollectionName<TState>());
        this.failureHandler = failureHandler;
        this.modelName = ProjectionModelName.For<TState>();
    }

    public async Task<TState?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        return await this.collection.Find(x => x.Id == modelId).SingleOrDefaultAsync(cancellationToken);
    }

    public async Task PersistBatch(ModelStatesBatch<TState> batch, CancellationToken cancellationToken)
    {
        ModelState<TState>[] changesToPersist = batch.Changes
                                                    .Where(x => !(x.IsNew && x.ShouldDelete))
                                                    .ToArray();

        TState[] inserts = changesToPersist
                           .Where(x => x is { IsNew: true, ShouldDelete: false, })
                           .Select(x => x.Model)
                           .ToArray();

        ModelState<TState>[] updatesAndDeletes = changesToPersist
                                                 .Where(x => !x.IsNew)
                                                 .ToArray();

        await this.InsertBatch(inserts, cancellationToken);
        await this.PersistUpdatesAndDeletes(updatesAndDeletes, cancellationToken);
    }

    private async Task InsertBatch(IReadOnlyCollection<TState> models, CancellationToken cancellationToken)
    {
        if (models.Count == 0)
        {
            return;
        }

        try
        {
            await this.collection.InsertManyAsync(
                models,
                new InsertManyOptions
                {
                    IsOrdered = false,
                },
                cancellationToken);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            await this.InsertOneAtATime(models, cancellationToken);
        }
    }

    private async Task InsertOneAtATime(IEnumerable<TState> models, CancellationToken cancellationToken)
    {
        foreach (TState model in models)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await this.Insert(model, cancellationToken);
        }
    }

    private async Task PersistUpdatesAndDeletes(IEnumerable<ModelState<TState>> modelStates, CancellationToken cancellationToken)
    {
        foreach (ModelState<TState> modelState in modelStates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                if (modelState.ShouldDelete)
                {
                    await this.Delete(modelState, cancellationToken);
                }
                else
                {
                    await this.Update(modelState, cancellationToken);
                }
            }
            catch (Exception ex)
            {
                ProjectionFailure failure = this.CreateFailure(modelState.Model, ex);
                await this.failureHandler.Handle(failure, cancellationToken);
            }
        }
    }

    private async Task Insert(TState model, CancellationToken cancellationToken)
    {
        try
        {
            await this.collection.InsertOneAsync(model, cancellationToken: cancellationToken);
        }
        catch (MongoWriteException ex) when (ex.WriteError.Category == ServerErrorCategory.DuplicateKey)
        {
            // Replay may re-attempt inserts after model persistence has already succeeded.
        }
    }

    private async Task Update(ModelState<TState> modelState, CancellationToken cancellationToken)
    {
        ReplaceOneResult result = await this.collection.ReplaceOneAsync(
                                      x => x.Id == modelState.Model.Id && x.EventNumber == modelState.ExpectedEventNumber,
                                      modelState.Model,
                                      cancellationToken: cancellationToken);

        if (result.MatchedCount == 0)
        {
            return;
        }
    }

    private async Task Delete(ModelState<TState> modelState, CancellationToken cancellationToken)
    {
        DeleteResult result = await this.collection.DeleteOneAsync(
                                  x => x.Id == modelState.Model.Id && x.EventNumber == modelState.ExpectedEventNumber,
                                  cancellationToken);

        if (result.DeletedCount == 0)
        {
            return;
        }
    }

    private ProjectionFailure CreateFailure(TState model, Exception exception)
    {
        long eventNumber = model.EventNumber
                           ?? throw new InvalidOperationException("Failed projection model must have an event number.");

        return new ProjectionFailure(
            modelId: model.Id,
            eventNumber: eventNumber,
            streamPosition: model.GlobalEventPosition,
            failureCreatedOn: DateTime.UtcNow,
            exception: exception.ToString(),
            failureType: nameof(FailureType.Persistence),
            modelName: this.modelName);
    }
}
