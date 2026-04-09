using System.Text;
using Newtonsoft.Json;

namespace Kuna.StreamGenerator;

internal static class EventStoreJson
{
    internal static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    internal static byte[] Serialize(object value)
    {
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value, SerializerSettings));
    }
}
