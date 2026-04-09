using System.ComponentModel.DataAnnotations.Schema;

namespace Kuna.Projections.Abstractions.Models;

/// <summary>
/// Optional base type for child entities that participate in detached
/// hierarchical projection persistence.
/// </summary>
/// <remarks>
/// This type exists only to help the sink classify detached child entities
/// during flush. New child instances default to
/// <see cref="EntityPersistenceState.New"/>, which allows the sink to insert
/// them when they are appended to an already-persisted root model. When the sink
/// loads a graph from storage, or after a flush has succeeded, participating
/// child entities are marked <see cref="EntityPersistenceState.Persisted"/>.
///
/// <para>
/// Use this base type only for child entities in hierarchical model graphs when
/// those child entities may be added after the root model has already been
/// stored. Flat models do not need it, and root models do not need it because
/// their insert/update/delete status is already known from <c>ModelState</c>.
/// </para>
/// </remarks>
public abstract class ChildEntity
{
    /// <summary>
    /// Transient sink-only persistence state. This property is not mapped to the
    /// database.
    /// </summary>
    [NotMapped]
    public EntityPersistenceState PersistenceState { get; set; } = EntityPersistenceState.New;
}
