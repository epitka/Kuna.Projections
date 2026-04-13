using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core;

/// <summary>
/// Transitional adapter that exposes the current post-flush in-memory cache
/// through the new projection-cache lookup seam without changing persistence
/// behavior yet.
/// </summary>
internal sealed class ProjectionCacheCompatibilityAdapter<TState> : IProjectionCache<TState>
    where TState : class, IModel, new()
{
    private readonly IModelStateCache<TState> modelStateCache;

    public ProjectionCacheCompatibilityAdapter(IModelStateCache<TState> modelStateCache)
    {
        this.modelStateCache = modelStateCache;
    }

    public ValueTask<ProjectedStateEnvelope<TState>?> Get(Guid modelId, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!this.modelStateCache.TryGet(modelId, out var cached)
            || cached == null)
        {
            return ValueTask.FromResult<ProjectedStateEnvelope<TState>?>(null);
        }

        return ValueTask.FromResult<ProjectedStateEnvelope<TState>?>(
            new ProjectedStateEnvelope<TState>(
                cached.Model,
                cached.IsNew,
                cached.ShouldDelete,
                cached.GlobalEventPosition,
                cached.ExpectedEventNumber,
                StageToken: 0,
                PersistenceStatus: ProjectionPersistenceStatus.Persisted));
    }

    public ValueTask Stage(ProjectedStateEnvelope<TState> state, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Staging through the projection cache adapter is not supported before the explicit cache stage is wired into the pipeline.");
    }

    public ValueTask<PersistencePullBatch<TState>?> PullNextBatch(
        PersistencePullRequest request,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Pull-based persistence is not supported through the projection cache adapter.");
    }

    public ValueTask CompletePull(
        PersistencePullBatch<TState> batch,
        IReadOnlyList<PersistenceItemOutcome> outcomes,
        CancellationToken cancellationToken)
    {
        throw new NotSupportedException("Per-item pull completion is not supported through the projection cache adapter.");
    }
}
