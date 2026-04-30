using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ModelDataStore<TState>
    : IModelStateSink<TState>,
      IModelStateStore<TState>
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<TState> collection;
    private readonly IProjectionFailureHandler<TState> failureHandler;
    private readonly string modelName;

    public ModelDataStore(
        ProjectionContext<TState> context,
        IProjectionFailureHandler<TState> failureHandler)
    {
        ClassMapRegistry.EnsureInitialized<TState>();

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
        var changesToPersist = batch.Changes
                                    .Where(x => x is not { IsNew: true, ShouldDelete: true, })
                                    .ToArray();

        var inserts = changesToPersist
                      .Where(x => x is { IsNew: true, ShouldDelete: false, })
                      .Select(x => x.Model);

        var updatesAndDeletes = changesToPersist
            .Where(x => !x.IsNew);

        await Task.WhenAll(
            this.InsertBatch(inserts, cancellationToken),
            this.PersistUpdatesAndDeletesBatch(updatesAndDeletes, cancellationToken));
    }

    private async Task InsertBatch(IEnumerable<TState> models, CancellationToken cancellationToken)
    {
        var modelsArray = models.ToArray();

        if (modelsArray.Length == 0)
        {
            return;
        }

        try
        {
            await this.collection.InsertManyAsync(
                modelsArray,
                new InsertManyOptions
                {
                    IsOrdered = false,
                },
                cancellationToken);
        }
        catch (MongoBulkWriteException<TState> ex) when (!cancellationToken.IsCancellationRequested)
        {
            await this.HandleBulkInsertFailure(modelsArray, ex, cancellationToken);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            await this.InsertOneAtATime(modelsArray, cancellationToken);
        }
    }

    private async Task InsertOneAtATime(IEnumerable<TState> models, CancellationToken cancellationToken)
    {
        foreach (var model in models)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await this.Insert(model, cancellationToken);
        }
    }

    private async Task PersistUpdatesAndDeletesBatch(
        IEnumerable<ModelState<TState>> modelStates,
        CancellationToken cancellationToken)
    {
        var modelStatesArray = modelStates.ToArray();

        if (modelStatesArray.Length == 0)
        {
            return;
        }

        var writes = modelStatesArray.Select(this.CreateWriteModel).ToArray();

        try
        {
            await this.collection.BulkWriteAsync(
                writes,
                new BulkWriteOptions
                {
                    IsOrdered = false,
                },
                cancellationToken);
        }
        catch (MongoBulkWriteException<TState> ex) when (!cancellationToken.IsCancellationRequested)
        {
            await this.HandleBulkUpdateDeleteFailure(modelStatesArray, ex, cancellationToken);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            await this.PersistUpdatesAndDeletesOneAtATime(modelStatesArray, cancellationToken);
        }
    }

    private async Task PersistUpdatesAndDeletesOneAtATime(
        IEnumerable<ModelState<TState>> modelStates,
        CancellationToken cancellationToken)
    {
        foreach (var modelState in modelStates)
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
                var failure = this.CreateFailure(modelState.Model, ex);
                await this.failureHandler.Handle(failure, cancellationToken);
            }
        }
    }

    private async Task HandleBulkUpdateDeleteFailure(
        IReadOnlyList<ModelState<TState>> modelStates,
        MongoBulkWriteException<TState> exception,
        CancellationToken cancellationToken)
    {
        HashSet<int> failedIndexes = [];

        foreach (var writeError in exception.WriteErrors)
        {
            if (writeError.Index < 0
                || writeError.Index >= modelStates.Count)
            {
                continue;
            }

            failedIndexes.Add(writeError.Index);

            var failure = this.CreateFailure(
                modelStates[writeError.Index].Model,
                new InvalidOperationException(writeError.Message, exception));

            await this.failureHandler.Handle(failure, cancellationToken);
        }

        if (exception.WriteConcernError is null)
        {
            return;
        }

        var modelStatesNeedingFallback = modelStates.Where((_, index) => !failedIndexes.Contains(index));
        await this.PersistUpdatesAndDeletesOneAtATime(modelStatesNeedingFallback, cancellationToken);
    }

    private async Task HandleBulkInsertFailure(
        IReadOnlyList<TState> models,
        MongoBulkWriteException<TState> exception,
        CancellationToken cancellationToken)
    {
        HashSet<int> failedIndexes = [];

        foreach (var writeError in exception.WriteErrors)
        {
            if (writeError.Index < 0
                || writeError.Index >= models.Count)
            {
                continue;
            }

            failedIndexes.Add(writeError.Index);

            if (writeError.Category == ServerErrorCategory.DuplicateKey)
            {
                continue;
            }

            var failure = this.CreateFailure(
                models[writeError.Index],
                new InvalidOperationException(writeError.Message, exception));

            await this.failureHandler.Handle(failure, cancellationToken);
        }

        if (exception.WriteConcernError is null)
        {
            return;
        }

        var modelsNeedingFallback = models.Where((_, index) => !failedIndexes.Contains(index));
        await this.InsertOneAtATime(modelsNeedingFallback, cancellationToken);
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
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            var failure = this.CreateFailure(model, ex);
            await this.failureHandler.Handle(failure, cancellationToken);
        }
    }

    private async Task Update(ModelState<TState> modelState, CancellationToken cancellationToken)
    {
        _ = await this.collection.ReplaceOneAsync(
                x => x.Id == modelState.Model.Id && x.EventNumber == modelState.ExpectedEventNumber,
                modelState.Model,
                cancellationToken: cancellationToken);
    }

    private async Task Delete(ModelState<TState> modelState, CancellationToken cancellationToken)
    {
        _ = await this.collection.DeleteOneAsync(
                x => x.Id == modelState.Model.Id && x.EventNumber == modelState.ExpectedEventNumber,
                cancellationToken);
    }

    private WriteModel<TState> CreateWriteModel(ModelState<TState> modelState)
    {
        var filter = Builders<TState>.Filter
                                     .Where(
                                         x => x.Id == modelState.Model.Id
                                              && x.EventNumber == modelState.ExpectedEventNumber);

        if (modelState.ShouldDelete)
        {
            return new DeleteOneModel<TState>(filter);
        }

        return new ReplaceOneModel<TState>(filter, modelState.Model);
    }

    private ProjectionFailure CreateFailure(TState model, Exception exception)
    {
        var eventNumber = model.EventNumber
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
