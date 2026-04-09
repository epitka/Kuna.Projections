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
        // Container-backed smoke test is opt-in to keep default test runs green on machines without Docker.
        if (!string.Equals(
                Environment.GetEnvironmentVariable("RUN_KURRENT_CONTAINER_TESTS"),
                "1",
                StringComparison.Ordinal))
        {
            return;
        }

        this.fixture.KurrentDBTestContainer.ShouldNotBeNull();
        this.fixture.ConnectionString.ShouldNotBeNullOrWhiteSpace();
    }
}
