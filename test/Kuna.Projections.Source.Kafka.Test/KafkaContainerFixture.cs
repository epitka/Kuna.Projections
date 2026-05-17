using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class KafkaContainerFixture
    : TestContainerFixture,
      IAsyncLifetime
{
    private readonly bool keepContainers;
    private readonly string containerName;
    private readonly KafkaContainer container;

    public KafkaContainerFixture()
    {
        this.keepContainers = string.Equals(
            Environment.GetEnvironmentVariable("KUNA_TEST_KEEP_CONTAINERS"),
            "1",
            StringComparison.Ordinal);

        var containerSuffix = Environment.GetEnvironmentVariable("KUNA_TEST_CONTAINER_SUFFIX") ?? "default";
        this.containerName = $"kuna-kafka-source-it-{containerSuffix}";

        var builder = new KafkaBuilder("confluentinc/cp-kafka:7.7.0")
                      .WithName(this.containerName)
                      .WithAutoRemove(!this.keepContainers)
                      .WithCleanUp(!this.keepContainers)
                      .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(9093));

        if (this.DockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(this.DockerEndpoint);
        }

        this.container = builder.Build();
    }

    public string BootstrapServers => this.container.GetBootstrapAddress();

    public async Task CreateTopicAsync(
        string topic,
        int partitions,
        CancellationToken cancellationToken)
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = this.BootstrapServers,
        }).Build();

        try
        {
            await adminClient.CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = partitions,
                        ReplicationFactor = 1,
                    },
                ]);
        }
        catch (CreateTopicsException ex)
            when (ex.Results.All(x => x.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

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
