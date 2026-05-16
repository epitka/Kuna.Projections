using MongoDB.Bson.Serialization.Attributes;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointDocument
{
    [BsonId]
    public required string Id { get; init; }

    public required string ModelName { get; init; }

    public required string InstanceId { get; init; }

    public string GlobalEventPosition { get; init; } = string.Empty;
}
