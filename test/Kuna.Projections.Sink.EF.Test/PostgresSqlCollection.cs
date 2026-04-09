using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

[CollectionDefinition(Name)]
public class PostgresSqlCollection : ICollectionFixture<PostgresSqlContainerFixture>
{
    public const string Name = "PostgresSql";
}
