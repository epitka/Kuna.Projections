using Kuna.Projections.Abstractions.Models;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class MongoProjectionContext<TState>
    where TState : class, IModel, new()
{
    public MongoProjectionContext(MongoProjectionOptions options)
    {
        this.Options = options;
        this.CollectionNamer = new CollectionNamer(options);
        this.Client = new MongoClient(options.ConnectionString);
        this.Database = this.Client.GetDatabase(options.DatabaseName);
    }

    public MongoProjectionOptions Options { get; }

    public CollectionNamer CollectionNamer { get; }

    public IMongoClient Client { get; }

    public IMongoDatabase Database { get; }
}
