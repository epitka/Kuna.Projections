namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureDocument
{
    public required string Id { get; init; }

    public required string ModelName { get; init; }

    public required string ModelId { get; init; }

    public required long EventNumber { get; init; }

    public required string GlobalEventPosition { get; init; }

    public required DateTime FailureCreatedOn { get; init; }

    public required string Exception { get; init; }

    public required string FailureType { get; init; }
}
