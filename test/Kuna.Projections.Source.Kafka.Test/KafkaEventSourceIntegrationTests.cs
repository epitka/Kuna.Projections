using System.Text;
using Confluent.Kafka;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

[Collection(KafkaCollection.Name)]
public sealed class KafkaEventSourceIntegrationTests
{
    private readonly KafkaContainerFixture fixture;

    public KafkaEventSourceIntegrationTests(KafkaContainerFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task ReadAll_Should_Consume_Kuna_Kafka_Record_From_Real_Broker()
    {
        var modelId = Guid.NewGuid();
        var topic = $"orders-events-{Guid.NewGuid():N}";
        await this.fixture.CreateTopicAsync(topic, partitions: 1, TestContext.Current.CancellationToken);

        using (var producer = new ProducerBuilder<byte[], byte[]>(
                   new ProducerConfig
                   {
                       BootstrapServers = this.fixture.BootstrapServers,
                   }).Build())
        {
            await producer.ProduceAsync(
                topic,
                new Message<byte[], byte[]>
                {
                    Key = Encoding.UTF8.GetBytes(modelId.ToString("D")),
                    Value = Encoding.UTF8.GetBytes("""{"value":"from-broker"}"""),
                    Headers =
                    [
                        new Header("event-type", Encoding.UTF8.GetBytes(nameof(TestEvent))),
                        new Header("event-number", Encoding.UTF8.GetBytes("7")),
                        new Header("created-on", Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z")),
                    ],
                },
                TestContext.Current.CancellationToken);
        }

        var source = new KafkaEventSource<TestModel>(
            new KafkaConsumerFactory(),
            new KunaKafkaSourceTransformer(),
            new KafkaEventEnvelopeFactory(new KafkaEventDeserializer([typeof(TestEvent),], NullLogger<KafkaEventDeserializer>.Instance)),
            new KafkaCheckpointSerializer(),
            new KafkaSourceSettings
            {
                BootstrapServers = this.fixture.BootstrapServers,
                Topic = topic,
                PollTimeoutMs = 50,
            },
            new ProjectionSettings<TestModel>
            {
                InstanceId = "orders-v1",
                Source = ProjectionSourceKind.Kafka,
            },
            NullLogger<KafkaEventSource<TestModel>>.Instance);

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(15));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token)
                                           .GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        var envelope = enumerator.Current;

        envelope.Event.ShouldBeOfType<TestEvent>().Value.ShouldBe("from-broker");
        envelope.EventNumber.ShouldBe(7);
        envelope.ModelId.ShouldBe(modelId);

        var checkpoint = new KafkaCheckpointSerializer().Deserialize(envelope.GlobalEventPosition);
        checkpoint.Topic.ShouldBe(topic);
        checkpoint.Partitions[0].ShouldBeGreaterThanOrEqualTo(0);
    }

    public sealed class TestEvent : Event
    {
        public string Value { get; init; } = string.Empty;
    }

    private sealed class TestModel : Model
    {
    }
}
