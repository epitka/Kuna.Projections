namespace Kuna.Projections.Abstractions.Models;

public class ProjectionFailure
{
    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    public ProjectionFailure()
    {
    }

    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    public ProjectionFailure(
        Guid modelId,
        long eventNumber,
        GlobalEventPosition streamPosition,
        DateTime failureCreatedOn,
        string exception,
        string failureType,
        string modelName)
    {
        this.ModelId = modelId;
        this.EventNumber = eventNumber;
        this.GlobalEventPosition = streamPosition;
        this.FailureCreatedOn = failureCreatedOn;
        this.Exception = exception;
        this.FailureType = failureType;
        this.ModelName = modelName;
    }

    public Guid ModelId { get; set; }

    /// <summary>
    /// Last event number in stream applied to projection.
    /// </summary>
    public long EventNumber { get; set; }

    /// <summary>
    /// Global position of the event in stream, used for checkpointing.
    /// </summary>
    public GlobalEventPosition GlobalEventPosition { get; set; }

    public DateTime FailureCreatedOn { get; }

    public required string Exception { get; set; } = string.Empty;

    public required string FailureType { get; set; } = string.Empty;

    public required string ModelName { get; set; } = string.Empty;
}

public enum FailureType
{
    Persistence,
    EventProcessing,
    EventDeserialization,
    EventOutOfOrder,
}
