using DotNet.Testcontainers.Containers;
using Testcontainers.KurrentDb;
using Xunit;

namespace Kuna.Projections.Source.KurrentDB.Test;

public class KurrentDBContainerFixture
    : TestContainerFixture,
      IAsyncLifetime
{
    private readonly bool keepContainers;
    private readonly string containerName;

    public KurrentDBContainerFixture()
    {
        this.keepContainers = string.Equals(
            Environment.GetEnvironmentVariable("KUNA_TEST_KEEP_CONTAINERS"),
            "1",
            StringComparison.Ordinal);

        var containerSuffix = Environment.GetEnvironmentVariable("KUNA_TEST_CONTAINER_SUFFIX") ?? "default";
        this.containerName = $"kuna-kurrent-source-it-{containerSuffix}";

        var builder = new KurrentDbBuilder("kurrentplatform/kurrentdb:25.1")
                      .WithName(this.containerName)
                      .WithAutoRemove(!this.keepContainers)
                      .WithCleanUp(!this.keepContainers);

        if (this.DockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(this.DockerEndpoint);
        }

        this.KurrentDBTestContainer = builder.Build();
    }

    public static KurrentDbBuilder KurrentDBContainerBuilder => new KurrentDbBuilder("kurrentplatform/kurrentdb:25.1")
                                                                .WithPortBinding("2117", "2117")
                                                                .WithExposedPort(2117);

    public IContainer KurrentDBTestContainer { get; }

    public string ConnectionString => ((KurrentDbContainer)this.KurrentDBTestContainer).GetConnectionString();

    public async ValueTask InitializeAsync()
    {
        if (!this.keepContainers)
        {
            await this.RemoveContainerIfPresentAsync(this.containerName);
        }

        await this.KurrentDBTestContainer.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (this.keepContainers)
        {
            return;
        }

        await this.KurrentDBTestContainer.StopAsync();
        await this.KurrentDBTestContainer.DisposeAsync();
    }
}
