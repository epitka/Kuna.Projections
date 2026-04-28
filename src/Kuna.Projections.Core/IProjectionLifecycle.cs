using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core;

/// <summary>
/// Defines internal lifecycle hooks that let the pipeline notify projection
/// runtime components about significant processing state transitions.
/// </summary>
internal interface IProjectionLifecycle<TState>
    where TState : class, IModel, new()
{
    /// <summary>
    /// Notifies runtime components that a flush completed successfully for the
    /// provided flushed model ids.
    /// The ids are not limited to models that produced a persisted
    /// <c>ModelState</c>. They also include models that participated in the
    /// flush window but were skipped, already marked as failed, or otherwise
    /// produced no persisted state change.
    /// <paramref name="clearModelIds"/> identifies the subset whose live
    /// runtime state can be discarded immediately after the flush.
    /// <paramref name="flushedEventNumbers"/> carries the flushed stream-local
    /// event number for models that produced persisted state. Runtime cleanup
    /// must not remove a live projection that has already advanced beyond that
    /// event number while the flush was in progress.
    /// </summary>
    void OnFlushSucceeded(
        IReadOnlyCollection<Guid> flushedModelIds,
        IReadOnlyCollection<Guid> clearModelIds,
        IReadOnlyDictionary<Guid, long?> flushedEventNumbers);
}
