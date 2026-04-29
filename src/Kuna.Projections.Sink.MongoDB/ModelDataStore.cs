using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ModelDataStore<TState> : IModelStateSink<TState>, IModelStateStore<TState>
    where TState : class, IModel, new()
{
    private readonly IMongoCollection<TState> collection;

    public ModelDataStore(IMongoDatabase database, CollectionNamer collectionNamer)
    {
        MongoModelClassMapRegistry.EnsureInitialized<TState>();
        this.collection = database.GetCollection<TState>(collectionNamer.GetModelCollectionName<TState>());
    }

    public async Task<TState?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        return await this.collection.Find(x => x.Id == modelId).SingleOrDefaultAsync(cancellationToken);
    }

    public Task PersistBatch(ModelStatesBatch<TState> batch, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
