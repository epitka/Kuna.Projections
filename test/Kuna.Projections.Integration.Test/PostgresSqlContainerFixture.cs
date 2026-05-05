using DotNet.Testcontainers.Builders;
using Testcontainers.PostgreSql;
using Xunit;

namespace Kuna.Projections.Pipeline.Integration.Test;

public sealed class PostgresSqlContainerFixture
    : TestContainerFixture,
      IAsyncLifetime
{
    private readonly bool keepContainers;
    private readonly PostgreSqlContainer container;
    private readonly string containerName;

    public PostgresSqlContainerFixture()
    {
        this.keepContainers = string.Equals(
            Environment.GetEnvironmentVariable("KUNA_TEST_KEEP_CONTAINERS"),
            "1",
            StringComparison.Ordinal);

        var containerSuffix = Environment.GetEnvironmentVariable("KUNA_TEST_CONTAINER_SUFFIX") ?? "default";
        this.containerName = $"kuna-postgres-it-{containerSuffix}";

        var builder = new PostgreSqlBuilder("postgres:15-alpine")
                      .WithName(this.containerName)
                      .WithDatabase("testdb")
                      .WithUsername("postgres")
                      .WithPassword("testpass")
                      .WithAutoRemove(!this.keepContainers)
                      .WithCleanUp(!this.keepContainers)
                      .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(5432))
                      .WithExposedPort(5432);

        if (this.DockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(this.DockerEndpoint);
        }

        this.container = builder.Build();
    }

    public string ConnectionString => this.container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        if (!this.keepContainers)
        {
            await this.RemoveContainerIfPresentAsync(this.containerName);
        }

        await this.container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (this.keepContainers)
        {
            return;
        }

        await this.container.DisposeAsync();
    }
}
