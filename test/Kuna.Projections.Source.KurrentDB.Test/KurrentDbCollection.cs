using Xunit;

namespace Kuna.Projections.Source.KurrentDB.Test;

[CollectionDefinition(Name)]
public class KurrentDbCollection : ICollectionFixture<KurrentDBContainerFixture>
{
    public const string Name = "KurrentDb";
}
