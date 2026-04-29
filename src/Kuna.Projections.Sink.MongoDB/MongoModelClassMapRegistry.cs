using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;

namespace Kuna.Projections.Sink.MongoDB;

internal static class MongoModelClassMapRegistry
{
    private static readonly Lock SyncRoot = new();

    public static void EnsureInitialized<TState>()
        where TState : class, IModel, new()
    {
        lock (SyncRoot)
        {
            if (BsonClassMap.IsClassMapRegistered(typeof(TState)))
            {
                return;
            }

            BsonClassMap.RegisterClassMap<TState>(
                classMap =>
                {
                    classMap.AutoMap();
                    classMap.MapIdMember(model => model.Id).SetSerializer(new MongoIdStringSerializer());
                });
        }
    }
}
