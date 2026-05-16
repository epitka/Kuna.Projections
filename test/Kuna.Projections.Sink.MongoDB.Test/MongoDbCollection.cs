using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[CollectionDefinition(Name)]
public sealed class MongoDbCollection : ICollectionFixture<MongoDbContainerFixture>
{
    public const string Name = "mongodb";
}
