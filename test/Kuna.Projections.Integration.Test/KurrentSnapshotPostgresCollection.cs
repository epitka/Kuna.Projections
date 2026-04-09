using Xunit;

namespace Kuna.Projections.Pipeline.Integration.Test;

[CollectionDefinition(Name)]
public class KurrentSnapshotPostgresCollection
    : ICollectionFixture<KurrentDBContainerFixture>,
      ICollectionFixture<PostgresSqlContainerFixture>
{
    public const string Name = "KurrentSnapshotPostgres";
}
