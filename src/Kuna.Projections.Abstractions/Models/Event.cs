namespace Kuna.Projections.Abstractions.Models;

public abstract class Event
{
    /// <summary>
    /// DateTime event was created in the event store.
    /// </summary>
    public DateTime CreatedOn { get; set; }

    /// <summary>
    /// Event type name
    /// </summary>
    public required string TypeName { get; set; }
}
