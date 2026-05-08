using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionFailureSerializer : ClassSerializerBase<ProjectionFailure>
{
    public override ProjectionFailure Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var document = BsonDocumentSerializer.Instance.Deserialize(context, args);

        return new ProjectionFailure(
            modelId: Guid.Empty,
            eventNumber: document[nameof(ProjectionFailure.EventNumber)].AsInt64,
            streamPosition: new GlobalEventPosition(document[nameof(ProjectionFailure.GlobalEventPosition)].AsString),
            failureCreatedOn: document[nameof(ProjectionFailure.FailureCreatedOn)].ToUniversalTime(),
            exception: document[nameof(ProjectionFailure.Exception)].AsString,
            failureType: document[nameof(ProjectionFailure.FailureType)].AsString,
            modelName: string.Empty,
            instanceId: string.Empty);
    }

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, ProjectionFailure value)
    {
        BsonDocumentSerializer.Instance.Serialize(
            context,
            args,
            new BsonDocument
            {
                { nameof(ProjectionFailure.EventNumber), value.EventNumber },
                { nameof(ProjectionFailure.GlobalEventPosition), value.GlobalEventPosition.ToString() },
                { nameof(ProjectionFailure.FailureCreatedOn), value.FailureCreatedOn },
                { nameof(ProjectionFailure.Exception), value.Exception },
                { nameof(ProjectionFailure.FailureType), value.FailureType },
            });
    }
}
