using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class KafkaHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_Should_Return_Healthy_When_Registered_Topics_Are_Reachable()
    {
        var consumerFactory = new FakeKafkaConsumerFactory(
            new FakeKafkaConsumer(new Dictionary<string, IReadOnlyList<int>>
            {
                ["orders-events"] = [0, 1],
            }));

        var healthCheck = new KafkaHealthCheck(
            [
                new KafkaHealthCheckRegistration
                {
                    SettingsSectionName = "OrdersProjection",
                    SourceSettings = new KafkaSourceSettings
                    {
                        BootstrapServers = "localhost:9092",
                        Topic = "orders-events",
                    },
                },
            ],
            consumerFactory);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Healthy);
        consumerFactory.RequestedConsumerGroups.ShouldBe(["kuna-projections-healthcheck-OrdersProjection",]);
    }

    [Fact]
    public async Task CheckHealthAsync_Should_Return_Unhealthy_When_Topic_Has_No_Partitions()
    {
        var healthCheck = new KafkaHealthCheck(
            [
                new KafkaHealthCheckRegistration
                {
                    SettingsSectionName = "OrdersProjection",
                    SourceSettings = new KafkaSourceSettings
                    {
                        BootstrapServers = "localhost:9092",
                        Topic = "orders-events",
                    },
                },
            ],
            new FakeKafkaConsumerFactory(new FakeKafkaConsumer(new Dictionary<string, IReadOnlyList<int>>())));

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull();
        result.Description.ShouldContain("orders-events");
    }

    [Fact]
    public async Task CheckHealthAsync_Should_Return_Unhealthy_When_Probe_Throws()
    {
        var healthCheck = new KafkaHealthCheck(
            [
                new KafkaHealthCheckRegistration
                {
                    SettingsSectionName = "OrdersProjection",
                    SourceSettings = new KafkaSourceSettings
                    {
                        BootstrapServers = "localhost:9092",
                        Topic = "orders-events",
                    },
                },
            ],
            new ThrowingKafkaConsumerFactory());

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Exception.ShouldNotBeNull();
    }

    [Fact]
    public async Task CheckHealthAsync_Should_Return_Unhealthy_When_Configured_Partition_Is_Missing()
    {
        var healthCheck = new KafkaHealthCheck(
            [
                new KafkaHealthCheckRegistration
                {
                    SettingsSectionName = "OrdersProjection",
                    SourceSettings = new KafkaSourceSettings
                    {
                        BootstrapServers = "localhost:9092",
                        Topic = "orders-events",
                        Partitions = [1,],
                    },
                },
            ],
            new FakeKafkaConsumerFactory(
                new FakeKafkaConsumer(
                    new Dictionary<string, IReadOnlyList<int>>
                    {
                        ["orders-events"] = [0,],
                    })));

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldNotBeNull();
        result.Description.ShouldContain("does not contain partitions");
    }

    private sealed class FakeKafkaConsumerFactory : IKafkaConsumerFactory
    {
        private readonly IKafkaConsumer consumer;

        public FakeKafkaConsumerFactory(IKafkaConsumer consumer)
        {
            this.consumer = consumer;
        }

        public List<string> RequestedConsumerGroups { get; } = [];

        public IKafkaConsumer Create(KafkaSourceSettings sourceSettings, string consumerGroupId)
        {
            this.RequestedConsumerGroups.Add(consumerGroupId);
            return this.consumer;
        }
    }

    private sealed class ThrowingKafkaConsumerFactory : IKafkaConsumerFactory
    {
        public IKafkaConsumer Create(KafkaSourceSettings sourceSettings, string consumerGroupId)
        {
            throw new InvalidOperationException("boom");
        }
    }

    private sealed class FakeKafkaConsumer : IKafkaConsumer
    {
        private readonly IReadOnlyDictionary<string, IReadOnlyList<int>> partitionsByTopic;

        public FakeKafkaConsumer(IReadOnlyDictionary<string, IReadOnlyList<int>> partitionsByTopic)
        {
            this.partitionsByTopic = partitionsByTopic;
        }

        public IReadOnlyList<int> GetPartitions(string topic)
        {
            return this.partitionsByTopic.TryGetValue(topic, out var partitions)
                ? partitions
                : [];
        }

        public void Assign(string topic, IReadOnlyCollection<int> partitions)
        {
        }

        public void Seek(string topic, int partition, long offset)
        {
        }

        public KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return null;
        }

        public long GetHighWatermarkOffset(string topic, int partition)
        {
            return 0;
        }

        public void Close()
        {
        }

        public void Dispose()
        {
        }
    }
}
