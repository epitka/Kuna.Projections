using DotNet.Testcontainers.Builders;
using Testcontainers.MongoDb;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

public sealed class MongoDbContainerFixture
    : TestContainerFixture,
      IAsyncLifetime
{
    private readonly MongoDbContainer container;

    public MongoDbContainerFixture()
    {
        var builder = new MongoDbBuilder("mongo:8.0")
                      .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(27017))
                      .WithExposedPort(27017);

        if (this.DockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(this.DockerEndpoint);
        }

        this.container = builder.Build();
    }

    public string ConnectionString => this.container.GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        await this.container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await this.container.DisposeAsync();
    }
}
