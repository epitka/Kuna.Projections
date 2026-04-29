using System.Globalization;
using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class GlobalEventPositionStringSerializer : StructSerializerBase<GlobalEventPosition>
{
    public override GlobalEventPosition Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        string value = context.Reader.ReadString();

        if (!ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out ulong parsedValue))
        {
            throw new FormatException($"Could not parse GlobalEventPosition from '{value}'.");
        }

        return new GlobalEventPosition(parsedValue);
    }

    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, GlobalEventPosition value)
    {
        context.Writer.WriteString(value.Value.ToString(CultureInfo.InvariantCulture));
    }
}
