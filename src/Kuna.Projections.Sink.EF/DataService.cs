using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using EFCore.BulkExtensions;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core;
using Kuna.Projections.Core.Models;
using Kuna.Projections.Core.Services;
using Kuna.Projections.EF.Data;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.EF
{
    public class DataService<TModel, TDataContext>

        where TModel : class, IModel, new()
        where TDataContext : DbContext, IProjectionDbContext
    {
        private readonly IServiceProvider serviceProvider;
        private readonly IProjectionFailureHandler<TModel> failureHandler;
        private readonly Stopwatch insertStopWatch;
        private readonly Stopwatch updateStopWatch;
        private readonly ILogger logger;
        private readonly List<ProjectionFailure> pendingUpdateFailures;

        private TDataContext dbContext;
        private IServiceScope scope;

        public DataService(
            IServiceProvider serviceProvider,
            IProjectionFailureHandler<TModel> failureHandler,
            ILogger<DataService<TModel, TDataContext>> logger)
        {
            this.serviceProvider = serviceProvider;
            this.failureHandler = failureHandler;
            this.logger = logger;

            this.InitDbContext();
            this.pendingUpdateFailures = new List<ProjectionFailure>();

            this.insertStopWatch = new Stopwatch();
            this.updateStopWatch = new Stopwatch();
        }

        public async Task<CheckPoint> GetCheckPoint()
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = (TDataContext)transientScope
                                                             .ServiceProvider
                                                             .GetService(typeof(TDataContext));

            var checkPoint = await transientContext!.CheckPoint
                                                    .FindAsync(typeof(TModel).Name);

            // NOTE: this will effectively require that checkpoint be added for projection via migration scripts or manually.
            if (checkPoint == null)
            {
                throw new Exception($"Checkpoint not found for {typeof(TModel).Name}");
            }

            return checkPoint;
        }

        public async Task PersistCheckPoint(CheckPoint checkPoint)
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = (TDataContext)transientScope
                                                             .ServiceProvider
                                                             .GetService(typeof(TDataContext));

            // using single here, as we only expect to ever get one checkpoint.
            var currentCheckpoint = await transientContext
                !.CheckPoint
                 .Where(x => x.ModelName == checkPoint.ModelName)
                 .SingleOrDefaultAsync();

            if (currentCheckpoint == null)
            {
                transientContext.Add(checkPoint);
            }
            else
            {
                currentCheckpoint.StreamPosition = checkPoint.StreamPosition;
            }

            await transientContext.SaveChangesAsync();
        }

        public async Task<TModel> Load(Guid modelId)
        {
            try
            {
                var model = await this.dbContext!.FindAsync<TModel>(modelId);

                return model;
            }
            catch (Exception ex)
            {
                this.logger.LogError(
                    "LoadModel failed for modelName: {Model} id: {ModelId}, with {Exception}",
                    typeof(TModel).Name,
                    modelId,
                    ex.ToString());

                throw;
            }
        }

        /// <summary>
        /// Persist batch processes all inserts, updates and deletes
        /// Items that have IsNew flag set to true will be bulk inserted. If item has both IsNew and ShouldDelete set to true
        /// they will be removed from the batch.
        /// All other items will be either updated or deleted.
        /// </summary>
        /// <param name="projections">Projection Array.</param>
        /// <returns>Task.</returns>
        public virtual async Task PersistBatch(Projection<TModel>[] projections)
        {
            // EP: let's remove items that are both IsNew and ShouldDelete
            // no reason to insert them and immediately delete them
            var toExclude = projections
                .Where(x => x.IsNew && x.ShouldDelete);

            await this.PersistBatchInternal(projections.Except(toExclude).ToArray());
            this.DisposeScope();
            this.InitDbContext();
        }

        public virtual async Task PersistOne(Projection<TModel> projection)
        {
            try
            {
                using var transientScope = this.serviceProvider.CreateScope();

                await using var transientContext = (TDataContext)transientScope
                                                                 .ServiceProvider
                                                                 .GetService(typeof(TDataContext));

                if (projection.IsNew)
                {
                    transientContext!.Attach(projection.Model);
                    transientContext!.Entry(projection.Model).State = EntityState.Added;
                    await transientContext.SaveChangesAsync();
                }
                else
                {
                    await this.SaveUpdates(new List<TModel> { projection.Model });
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError(
                    "Failed to persistOne of stream projection {model} for {ModelId}, exception {Exception}",
                    typeof(TModel).Name,
                    projection.Model.Id,
                    ex.ToString());

                var failure = new ProjectionFailure(
                    modelId: projection.Model.Id,
                    exception: ex.ToString(),
                    failureType: nameof(FailureType.Persistence),
                    eventNumberInStream: projection.Model.EventNumber!.Value,
                    streamPosition: projection.Model.StreamPosition,
                    modelName: typeof(TModel).Name,
                    failureCreatedOn: DateTime.Now.ToUniversalTime());

                await this.failureHandler.Handle(failure);
            }

            // EP: this is very aggressive, maybe we could implement some strategy to evict items
            // that have not had any activity for a while, or maybe once we reach certain size, some FIFO method.
            this.DisposeScope();
            this.InitDbContext();
        }

        public async Task Delete(Projection<TModel> projection)
        {
            try
            {
                using var transientScope = this.serviceProvider.CreateScope();

                await using var transientContext = (TDataContext)transientScope
                                                                 .ServiceProvider
                                                                 .GetService(typeof(TDataContext));

                transientContext!.Attach(projection.Model);
                transientContext!.Entry(projection.Model).State = EntityState.Deleted;
                await transientContext.SaveChangesAsync();
            }
            catch (Exception e)
            {
                // NOTE: for now swallowing if not able to delete. Should not stop projection as delete is
                // is just to reduce number of entries, more of a optimization than anything else.
                this.logger.LogError(
                    "Failed to delete stream projection {ModelName} as faulted for {ModelId}, exception {Exception}",
                    typeof(TModel).Name,
                    projection.Model.Id,
                    e.ToString());
            }
        }

        public async Task BulkDelete(Projection<TModel>[] projections)
        {
            var tryIndividually = false;

            if (projections.Length == 0)
            {
                return;
            }

            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = (TDataContext)transientScope
                                                             .ServiceProvider
                                                             .GetService(typeof(TDataContext));

            var strategy = transientContext!.Database.CreateExecutionStrategy();

            var models = projections.Select(x => x.Model)
                                    .ToArray();

            foreach (var model in models)
            {
                transientContext!.Attach(model);
                transientContext!.Entry(model).State = EntityState.Deleted;
            }

            await strategy.ExecuteAsync(
                async () =>
                {
                    await using var transaction = await transientContext!.Database.BeginTransactionAsync();

                    try
                    {
                        // EP: we are not using BulkDeleteAsync as it will not delete child objects
                        // when using transient context
                        await transientContext.SaveChangesAsync();
                        await transaction.CommitAsync();
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogInformation(ex, "Could not bulk delete entities, trying individually");
                        await transaction.RollbackAsync();

                        tryIndividually = true;
                    }
                });

            if (tryIndividually == false)
            {
                return;
            }

            var tasks = new List<Task>();

            // EP: there is a possibility that individual ones fail as well
            // for now ignoring that
            foreach (var model in projections)
            {
                tasks.Add(this.Delete(model));
            }

            await Task.WhenAll(tasks);
        }

        public async Task PersistReplayed(Projection<TModel> projection)
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = (TDataContext)transientScope
                                                             .ServiceProvider
                                                             .GetService(typeof(TDataContext));

            // let's first delete this projection
            var toRemove = await transientContext!.FindAsync(typeof(TModel), projection.Model.Id);

            if (toRemove != null)
            {
                transientContext!.Remove(toRemove);

                await transientContext.SaveChangesAsync();
            }

            // now lets fetch failure so we can delete it and update model with replayed values
            // these 2 operations run in transaction
            var failure = (await transientContext.ProjectionFailures
                                                 .Where(x => x.ModelId == projection.Model.Id)
                                                 .ToListAsync())
                .FirstOrDefault();

            if (failure != null)
            {
                transientContext.ProjectionFailures.Remove(failure);
            }

            // EP: it is possible that when saving this version of model
            // that another type of failure occurs on the same model
            // need to see how to handle that, we probably want to attach it to the existing failure
            // for now let's just log error.
            transientContext.Add(projection.Model);

            try
            {
                await transientContext.SaveChangesAsync();
            }
            catch (Exception e)
            {
                this.logger.LogError(
                    e,
                    "Failure to persist when replying the model {ModelName} for  {ModelId}",
                    nameof(TModel),
                    projection.Model.Id);
            }
        }

        public virtual async Task PersistBatchInternal(Projection<TModel>[] projections)
        {
            // EP: while inserts can be processed immediately
            // updates have pre-processing step, to remove projections
            // flagged for deletion from dbContext, hence difference in toInsert vs. toUpdate
            var toInsert = new List<TModel>(projections.Length);

            var toUpdate = new List<Projection<TModel>>();

            for (var i = 0; i < projections.Length; i++)
            {
                if (projections[i].IsNew)
                {
                    toInsert.Add(projections[i].Model);
                    continue;
                }

                toUpdate.Add(projections[i]);
            }

            // bulk inserts has its own transient scope, so any inserts are not present in the current db context.
            // Due to EF being unable to identify changes of children objects, we need are no longer using bulkUpdates.
            // we are using the current this.dbcontext to save the updates to the db.
            // Sql Server locks the tables during inserts. calling the db transactions in order, vs in parallel fixes
            // any deadlocks that were previously observed.
            // Inserts are for new models, so they would not have been read from the db, and hence are not
            // present in dbContext. Also models that are marked as scheduled for deletion and are new will not be inserted at all.
            await this.DoBulkInserts(toInsert);
            await this.SaveUpdates(toUpdate);
        }

        private async Task DoBulkInserts(List<TModel> models)
        {
            if (models.Count == 0)
            {
                return;
            }

            var shouldRetry = false;

            using var transientScope = this.serviceProvider.CreateScope();

            await using (var transientContext = (TDataContext)transientScope
                                                              .ServiceProvider
                                                              .GetService(typeof(TDataContext)))
            {
                var strategy = transientContext!.Database.CreateExecutionStrategy();

                await strategy.ExecuteAsync(
                    async () =>
                    {
                        await using var transaction = await transientContext!.Database.BeginTransactionAsync();

                        try
                        {
                            await transientContext.BulkInsertAsync(
                                models,
                                options => { options.IncludeGraph = true; });

                            await transaction.CommitAsync();

                            this.logger.LogInformation(
                                "Inserted {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                                models.Count,
                                this.insertStopWatch.ElapsedMilliseconds.ToString("N0"),
                                typeof(TModel).Name);

                            this.insertStopWatch.Reset();
                        }

                        // EP: we have to catch generic exception here rather than DbUpdateException
                        // because BulkExtensions uses SqlBulkCopy and it will not throw DbUpdateException
                        // so we cannot apply strategy like we can for updates.
                        catch (Exception ex)
                        {
                            await transaction.RollbackAsync();

                            shouldRetry = true;
                            this.logger.LogWarning(ex, "Batch Update failed, retrying in chunks...");
                        }
                    });
            }

            if (shouldRetry)
            {
                var batchSize = models.Count / 2;

                var firstBatch = models.Take(batchSize).ToArray();
                var secondBatch = models.Skip(batchSize).ToArray();

                if (firstBatch.Length <= 10)
                {
                    await this.InsertOneAtTheTime(firstBatch);
                    await this.InsertOneAtTheTime(secondBatch);
                }
                else
                {
                    await this.DoBulkInserts(firstBatch.ToList());
                    await this.DoBulkInserts(secondBatch.ToList());
                }
            }
        }

        private async Task InsertOneAtTheTime(IEnumerable<TModel> models)
        {
            using var transientScope = this.serviceProvider.CreateScope();

            await using var transientContext = (TDataContext)transientScope
                                                             .ServiceProvider
                                                             .GetService(typeof(TDataContext));

            foreach (var model in models)
            {
                try
                {
                    transientContext!.Attach(model);
                    transientContext!.Entry(model).State = EntityState.Added;
                    await transientContext.SaveChangesAsync();
                }
                catch (Exception ex)
                {
                    this.logger.LogWarning(ex, "model {Model} failed to insert", typeof(TModel).Name);

                    var failure = new ProjectionFailure(
                        modelId: model.Id,
                        exception: ex.ToString(),
                        failureType: nameof(FailureType.Persistence),
                        eventNumberInStream: model.EventNumber!.Value,
                        streamPosition: model.StreamPosition,
                        modelName: typeof(TModel).Name,
                        failureCreatedOn: DateTime.Now.ToUniversalTime());

                    await this.failureHandler.Handle(failure);

                    // Need to detach the failed model from the context, so the next records can succeed.
                    transientContext!.Entry(model).State = EntityState.Detached;
                }
            }
        }

        private async Task SaveUpdates(List<Projection<TModel>> projections)
        {
            if (projections.Count == 0)
            {
                return;
            }

            var models = projections.Select(x => x.Model)
                                    .ToList();

            // EP: let's first remove items that are marked for deletion from dbContext
            // we are using running context as these items have been loaded from db and are being tracked in it
            foreach (var projection in projections.Where(projection => projection.ShouldDelete))
            {
                this.dbContext.Remove(projection.Model);
            }

            await this.SaveUpdates(models);
        }

        private async Task SaveUpdates(List<TModel> models)
        {
            // EP: DO NOT try using Bulk extensions for updates. We will not be able to process failures correctly
            // You cannot attach entity to transient dbContext and persist it correctly in the case of deep graph
            if (models.Count == 0)
            {
                return;
            }

            var strategy = this.dbContext.Database.CreateExecutionStrategy();

            await strategy.ExecuteAsync(
                async () =>
                {
                    await using var transaction = await this.dbContext.Database.BeginTransactionAsync();

                    try
                    {
                        await this.dbContext.SaveChangesAsync();

                        await transaction.CommitAsync();

                        this.logger.LogInformation(
                            "Updated {ModelCount} models in {SqlExecutionTime} milliseconds for {Model}",
                            models.Count,
                            this.updateStopWatch.ElapsedMilliseconds.ToString("N0"),
                            typeof(TModel).Name);
                    }
                    catch (DbUpdateException dbex)
                    {
                        await transaction.RollbackAsync();

                        if (dbex.Entries.Count > 0)
                        {
                            foreach (var failedEntry in dbex.Entries)
                            {
                                var model = failedEntry.Entity as TModel;

                                // in children were modified they will be in Entries collection as well
                                // ignore them
                                if (model is null)
                                {
                                    continue;
                                }

                                models.Remove(model);

                                this.logger.LogWarning(
                                    dbex,
                                    "Failed to update stream projection {ModelName} {@Model}",
                                    typeof(TModel).Name,
                                    (TModel)failedEntry.Entity);

                                // detach entity to prevent this from affecting subsequent save attempts
                                failedEntry.State = EntityState.Detached;

                                // TODO: EP: add serialized model state to the failure entry
                                var failure = new ProjectionFailure(
                                    modelId: model.Id,
                                    exception: dbex.ToString(),
                                    failureType: nameof(FailureType.Persistence),
                                    eventNumberInStream: model.EventNumber!.Value,
                                    streamPosition: model.StreamPosition,
                                    modelName: typeof(TModel).Name,
                                    failureCreatedOn: DateTime.Now.ToUniversalTime());

                                this.pendingUpdateFailures.Add(failure);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        await transaction.RollbackAsync();

                        this.logger.LogError(
                            ex,
                            "Failed to save updates for stream projection {Model}, number Of Models impacted {Modelcount}",
                            typeof(TModel).Name,
                            models.Count);

                        // dispose of the scope so the models are detached from the context.
                        this.DisposeScope();

                        // since we cannot identify models that have failed, whole batch will be marked as failed
                        foreach (var model in models)
                        {
                            this.logger.LogWarning(
                                ex,
                                "Failed to update stream projection {Model} {Modelid}",
                                typeof(TModel).Name,
                                model.Id);

                            var failure = new ProjectionFailure(
                                modelId: model.Id,
                                exception: ex.ToString(),
                                failureType: nameof(FailureType.Persistence),
                                eventNumberInStream: model.EventNumber!.Value,
                                streamPosition: model.StreamPosition,
                                modelName: typeof(TModel).Name,
                                failureCreatedOn: DateTime.Now.ToUniversalTime());

                            this.pendingUpdateFailures.Add(failure);
                        }

                        models.Clear();
                    }
                    finally
                    {
                        this.updateStopWatch.Reset();
                    }

                    if (this.pendingUpdateFailures.Any())
                    {
                        // now that we have collected failures, let's process them
                        foreach (var failure in this.pendingUpdateFailures)
                        {
                            await this.failureHandler.Handle(failure);
                        }

                        this.pendingUpdateFailures.Clear();

                        // now let's try to persist remaining entries that were not marked as failed
                        // we have removed all failed ones in the previous step.
                        await this.SaveUpdates(models);
                    }
                });
        }

        private void InitDbContext()
        {
            this.scope = this.serviceProvider.CreateScope();
            this.dbContext = (TDataContext)this.scope.ServiceProvider.GetService(typeof(TDataContext));
        }

        private void DisposeScope()
        {
            this.scope?.Dispose();
            this.scope = null;
        }
    }
}
