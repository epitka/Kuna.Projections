using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;

namespace Kuna.Projections.Sink.MongoDB;

internal static class SerializerRegistry
{
    private static readonly Lock SyncRoot = new();
    private static bool initialized;

    public static void EnsureInitialized()
    {
        lock (SyncRoot)
        {
            if (initialized)
            {
                return;
            }

            BsonSerializer.RegisterSerializer<Guid>(new IdSerializer());
            BsonSerializer.RegisterSerializer<GlobalEventPosition>(new GlobalEventPositionSerializer());

            initialized = true;
        }
    }
}
