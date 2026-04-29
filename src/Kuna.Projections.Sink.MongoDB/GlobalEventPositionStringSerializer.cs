using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class GlobalEventPositionStringSerializer : StructSerializerBase<GlobalEventPosition>
{
    public override GlobalEventPosition Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        string value = context.Reader.ReadString();
        return MongoGlobalEventPosition.Parse(value);
    }

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, GlobalEventPosition value)
    {
        context.Writer.WriteString(MongoGlobalEventPosition.Format(value));
    }
}
