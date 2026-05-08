namespace Kuna.Projections.Abstractions.Models;

public interface IModel
{
    Guid Id { get; init; }

    /// <summary>
    /// Event version number in aggregate stream.
    /// </summary>
    long? EventNumber { get; set; }

    /// <summary>
    /// Global position of the event
    /// </summary>
    GlobalEventPosition GlobalEventPosition { get; set; }

    /// <summary>
    /// A flag indicating whether the stream has faulted.
    /// </summary>
    bool HasStreamProcessingFaulted { get; set; }

    ProjectionFailure? ProjectionFailure { get; set; }
}

public abstract class Model
    : IModel
{
    public Guid Id { get; init; }

    public long? EventNumber { get; set; }

    public GlobalEventPosition GlobalEventPosition { get; set; }

    public bool HasStreamProcessingFaulted { get; set; }

    public ProjectionFailure? ProjectionFailure { get; set; }
}
