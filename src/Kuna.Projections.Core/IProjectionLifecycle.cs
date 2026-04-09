namespace Kuna.Projections.Core;

/// <summary>
/// Defines internal lifecycle hooks that let the pipeline notify projection
/// runtime components about significant processing state transitions.
/// </summary>
internal interface IProjectionLifecycle
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
    /// </summary>
    void OnFlushSucceeded(IReadOnlyCollection<Guid> flushedModelIds, IReadOnlyCollection<Guid> clearModelIds);
}
