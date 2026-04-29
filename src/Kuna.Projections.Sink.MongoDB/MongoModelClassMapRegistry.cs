using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;

namespace Kuna.Projections.Sink.MongoDB;

internal static class MongoModelClassMapRegistry
{
    private static readonly Lock SyncRoot = new();
    private static bool baseModelInitialized;

    public static void EnsureInitialized<TState>()
        where TState : class, IModel, new()
    {
        lock (SyncRoot)
        {
            EnsureBaseModelInitialized();

            if (BsonClassMap.IsClassMapRegistered(typeof(TState)))
            {
                return;
            }

            BsonClassMap.RegisterClassMap<TState>(
                classMap =>
                {
                    classMap.AutoMap();
                });
        }
    }

    private static void EnsureBaseModelInitialized()
    {
        if (baseModelInitialized || BsonClassMap.IsClassMapRegistered(typeof(Model)))
        {
            baseModelInitialized = true;
            return;
        }

        BsonClassMap.RegisterClassMap<Model>(
            classMap =>
            {
                classMap.AutoMap();
                classMap.SetIsRootClass(true);

                BsonMemberMap idMemberMap = classMap.GetMemberMap(nameof(Model.Id));
                idMemberMap.SetSerializer(new MongoIdStringSerializer());
                classMap.SetIdMember(idMemberMap);
            });

        baseModelInitialized = true;
    }
}
