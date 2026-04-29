using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class MongoIdStringSerializer : SerializerBase<Guid>
{
    public override Guid Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var value = context.Reader.ReadString();
        return MongoGuid.Parse(value);
    }

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Guid value)
    {
        context.Writer.WriteString(MongoGuid.Format(value));
    }
}
