using DotNet.Testcontainers.Builders;
using Testcontainers.PostgreSql;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

public sealed class PostgresSqlContainerFixture
    : TestContainerFixture,
      IAsyncLifetime
{
    private readonly PostgreSqlContainer container;

    public PostgresSqlContainerFixture()
    {
        this.IsEnabled = string.Equals(
            Environment.GetEnvironmentVariable("RUN_EF_CONTAINER_TESTS"),
            "1",
            StringComparison.Ordinal);

        var builder = new PostgreSqlBuilder("postgres:15-alpine")
                      .WithDatabase("testdb")
                      .WithUsername("kuna")
                      .WithPassword("testpass")
                      .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(5432))
                      .WithExposedPort(5432);

        if (this.DockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(this.DockerEndpoint);
        }

        this.container = builder.Build();
    }

    public bool IsEnabled { get; }

    public string ConnectionString => this.container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        if (!this.IsEnabled)
        {
            return;
        }

        await this.container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (!this.IsEnabled)
        {
            return;
        }

        await this.container.DisposeAsync();
    }
}
