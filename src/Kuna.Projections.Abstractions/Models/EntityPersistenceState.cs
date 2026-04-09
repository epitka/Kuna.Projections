namespace Kuna.Projections.Abstractions.Models;

/// <summary>
/// Transient persistence classification used by the EF projection sink when it
/// needs to attach a detached object graph back to a <see cref="Microsoft.EntityFrameworkCore.DbContext"/>
/// before flush.
/// </summary>
/// <remarks>
/// This state is not stored in the database. It exists only to tell the sink
/// whether a detached entity instance should be treated as a new row, an
/// already-persisted row, or a row that should be deleted.
/// </remarks>
public enum EntityPersistenceState
{
    /// <summary>
    /// The entity instance has not been persisted yet and should be inserted if
    /// it reaches the sink in a flush.
    /// </summary>
    New = 0,

    /// <summary>
    /// The entity instance already exists in durable projection storage.
    /// Entities loaded from the store, and entities that have already been
    /// flushed successfully, should be in this state.
    /// </summary>
    Persisted = 1,

    /// <summary>
    /// The entity instance represents a row that should be deleted.
    /// </summary>
    Deleted = 2,
}
