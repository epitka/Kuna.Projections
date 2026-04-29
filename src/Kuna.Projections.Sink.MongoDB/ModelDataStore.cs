using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ModelDataStore<TState> : IModelStateSink<TState>, IModelStateStore<TState>
    where TState : class, IModel, new()
{
    public Task<TState?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task PersistBatch(ModelStatesBatch<TState> batch, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
