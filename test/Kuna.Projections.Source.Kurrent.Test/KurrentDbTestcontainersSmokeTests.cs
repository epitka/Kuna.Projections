using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

[Collection(KurrentDbCollection.Name)]
public class KurrentDbTestcontainersSmokeTests
{
    private readonly KurrentDBContainerFixture fixture;

    public KurrentDbTestcontainersSmokeTests(KurrentDBContainerFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public void KurrentDb_Testcontainer_Starts_And_Provides_ConnectionString()
    {
        this.fixture.KurrentDBTestContainer.ShouldNotBeNull();
        this.fixture.ConnectionString.ShouldNotBeNullOrWhiteSpace();
    }
}
