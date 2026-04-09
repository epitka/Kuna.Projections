using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

[CollectionDefinition(Name)]
public class KurrentDbCollection : ICollectionFixture<KurrentDBContainerFixture>
{
    public const string Name = "KurrentDb";
}
