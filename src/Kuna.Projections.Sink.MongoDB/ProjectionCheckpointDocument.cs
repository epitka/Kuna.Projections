using MongoDB.Bson.Serialization.Attributes;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointDocument
{
    [BsonId]
    public required string ModelName { get; init; }

    public string GlobalEventPosition { get; init; } = string.Empty;
}
