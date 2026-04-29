using Kuna.Projections.Abstractions.Models;
using MongoDB.Driver;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionContext<TState>
    where TState : class, IModel, new()
{
    public ProjectionContext(ProjectionOptions options)
    {
        this.Options = options;
        this.CollectionNamer = new CollectionNamer(options);
        this.Client = new MongoClient(options.ConnectionString);
        this.Database = this.Client.GetDatabase(options.DatabaseName);
    }

    public ProjectionOptions Options { get; }

    public CollectionNamer CollectionNamer { get; }

    public IMongoClient Client { get; }

    public IMongoDatabase Database { get; }
}
