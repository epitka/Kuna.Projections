using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;

namespace Kuna.Projections.Sink.MongoDB;

internal static class MongoSerializationRegistry
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

            BsonSerializer.RegisterSerializer<Guid>(new MongoIdStringSerializer());
            BsonSerializer.RegisterSerializer<GlobalEventPosition>(new GlobalEventPositionStringSerializer());

            initialized = true;
        }
    }
}
