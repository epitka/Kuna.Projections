using Kuna.Projections.Abstractions.Models;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionContext<TState>
    where TState : class, IModel, new()
{
    private static readonly Lock SyncRoot = new();
    private static readonly Dictionary<string, IMongoClient> ClientsByConnectionString = new(StringComparer.Ordinal);

    public ProjectionContext(ProjectionOptions options, ICollectionNamer collectionNamer)
    {
        ClassMapRegistry.EnsureInitialized<TState>();

        this.Options = options;
        this.CollectionNamer = collectionNamer;
        this.Client = GetOrCreateClient(options.ConnectionString);
        this.Database = this.Client.GetDatabase(options.DatabaseName);
    }

    public ProjectionOptions Options { get; }

    public ICollectionNamer CollectionNamer { get; }

    public IMongoClient Client { get; }

    public IMongoDatabase Database { get; }

    private static IMongoClient GetOrCreateClient(string connectionString)
    {
        lock (SyncRoot)
        {
            if (ClientsByConnectionString.TryGetValue(connectionString, out var existingClient))
            {
                return existingClient;
            }

            var client = new MongoClient(connectionString);
            ClientsByConnectionString[connectionString] = client;
            return client;
        }
    }
}
