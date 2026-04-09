using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Core;

/// <summary>
/// Creates live <see cref="Projection{TState}"/> instances for the projection
/// engine. The engine uses this factory to start a brand-new projection for a
/// model id, recreate one by loading persisted model state from the state store,
/// or rebuild one from model state that is already available in memory.
/// </summary>
public interface IProjectionFactory<TState>
    where TState : class, IModel, new()
{
    /// <summary>
    /// Creates a projection for the given model id. When
    /// <paramref name="loadModelFromStore"/> is <see langword="true"/>, the
    /// implementation should attempt to load existing model state before
    /// returning a projection instance.
    /// </summary>
    ValueTask<Projection<TState>?> Create(Guid modelId, bool loadModelFromStore, CancellationToken cancellationToken);

    /// <summary>
    /// Recreates a projection from model state that is already available in
    /// memory, such as state restored from the in-memory model-state cache.
    /// </summary>
    Projection<TState> CreateFromModel(TState model, bool isNew);
}

/// <summary>
/// Default <see cref="IProjectionFactory{TState}"/> implementation. It creates
/// concrete projection instances with the registered constructor delegate and,
/// when requested, loads existing model state from the model-state store before
/// returning the projection to the engine.
/// </summary>
public class ProjectionFactory<TState> : IProjectionFactory<TState>
    where TState : class, IModel, new()
{
    private readonly Func<Guid, Projection<TState>> createProjectionFunc;
    private readonly IModelStateStore<TState> stateStore;

    public ProjectionFactory(
        Func<Guid, Projection<TState>> createProjectionFunc,
        IModelStateStore<TState> stateStore)
    {
        this.createProjectionFunc = createProjectionFunc;
        this.stateStore = stateStore;
    }

    public async ValueTask<Projection<TState>?> Create(
        Guid modelId,
        bool loadModelFromStore,
        CancellationToken cancellationToken)
    {
        if (!loadModelFromStore)
        {
            return this.createProjectionFunc(modelId);
        }

        var model = await this.stateStore.Load(modelId, cancellationToken);

        if (model == null)
        {
            return null;
        }

        var projection = this.createProjectionFunc(modelId);

        projection.IsNew = false;
        projection.SetModelState(model);

        return projection;
    }

    public Projection<TState> CreateFromModel(TState model, bool isNew)
    {
        var projection = this.createProjectionFunc(model.Id);
        projection.IsNew = isNew;
        projection.SetModelState(model);
        return projection;
    }
}
