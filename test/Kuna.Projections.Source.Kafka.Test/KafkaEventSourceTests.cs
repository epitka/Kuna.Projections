using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class KafkaEventSourceTests
{
    [Fact]
    public async Task ReadAll_Should_Seek_From_Checkpoint_And_Emit_Envelope()
    {
        var modelId = Guid.NewGuid();
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages:
            [
                new KafkaConsumedMessage
                {
                    Topic = "orders-events",
                    Partition = 0,
                    Offset = 6,
                    KeyBytes = System.Text.Encoding.UTF8.GetBytes(modelId.ToString("D")),
                    ValueBytes = System.Text.Encoding.UTF8.GetBytes("""{"value":"abc"}"""),
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["event-type"] = System.Text.Encoding.UTF8.GetBytes(nameof(TestEvent)),
                        ["event-number"] = System.Text.Encoding.UTF8.GetBytes("3"),
                        ["created-on"] = System.Text.Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                    },
                },
            ],
            highWatermarks: new Dictionary<int, long> { [0] = 7, });

        var source = CreateSource(
            consumer,
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        var checkpointSerializer = new KafkaCheckpointSerializer();
        var start = checkpointSerializer.Serialize(
            new KafkaCheckpointDocument
            {
                Topic = "orders-events",
                Partitions = new Dictionary<int, long> { [0] = 5, },
            });

        using var cts = new CancellationTokenSource();
        await using var enumerator = source.ReadAll(start, cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        var envelope = enumerator.Current;

        consumer.AssignedTopic.ShouldBe("orders-events");
        consumer.AssignedPartitions.ShouldBe([0,]);
        consumer.SeekCalls.ShouldContain(("orders-events", 0, 6L));
        envelope.Event.ShouldBeOfType<TestEvent>();
        envelope.EventNumber.ShouldBe(3);

        var emittedCheckpoint = checkpointSerializer.Deserialize(envelope.GlobalEventPosition);
        emittedCheckpoint.Partitions[0].ShouldBe(6);
    }

    [Fact]
    public async Task ReadAll_Should_Emit_CaughtUp_Event_When_No_Messages_And_End_Offsets_Are_Reached()
    {
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages: [],
            highWatermarks: new Dictionary<int, long> { [0] = 0, });

        var source = CreateSource(
            consumer,
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<ProjectionCaughtUpEvent>();
    }

    [Fact]
    public async Task ReadAll_Should_Throw_When_Checkpoint_Topic_Does_Not_Match_Configured_Topic()
    {
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages: [],
            highWatermarks: new Dictionary<int, long> { [0] = 0, });

        var source = CreateSource(
            consumer,
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        var checkpoint = new KafkaCheckpointSerializer().Serialize(
            new KafkaCheckpointDocument
            {
                Topic = "payments-events",
                Partitions = new Dictionary<int, long> { [0] = 5, },
            });

        var ex = await Should.ThrowAsync<InvalidOperationException>(
            async () =>
            {
                await foreach (var _ in source.ReadAll(checkpoint, CancellationToken.None))
                {
                }
            });

        ex.Message.ShouldContain("Checkpoint topic");
    }

    private static KafkaEventSource<TestModel> CreateSource(
        IKafkaConsumer consumer,
        KafkaSourceSettings sourceSettings)
    {
        return new KafkaEventSource<TestModel>(
            new FakeKafkaConsumerFactory(consumer),
            new NativeKafkaSourceTransformer(),
            new KafkaEventEnvelopeFactory(
                new KafkaEventDeserializer([typeof(TestEvent),], NullLogger<KafkaEventDeserializer>.Instance)),
            new KafkaCheckpointSerializer(),
            sourceSettings,
            new ProjectionSettings<TestModel>
            {
                InstanceId = "orders-v1",
                Source = ProjectionSourceKind.Kafka,
            },
            NullLogger<KafkaEventSource<TestModel>>.Instance);
    }

    public sealed class TestEvent : Event
    {
        public string Value { get; init; } = string.Empty;
    }

    private sealed class TestModel : Model
    {
    }

    private sealed class FakeKafkaConsumerFactory : IKafkaConsumerFactory
    {
        private readonly IKafkaConsumer consumer;

        public FakeKafkaConsumerFactory(IKafkaConsumer consumer)
        {
            this.consumer = consumer;
        }

        public IKafkaConsumer Create(KafkaSourceSettings sourceSettings, string consumerGroupId)
        {
            return this.consumer;
        }
    }

    private sealed class FakeKafkaConsumer : IKafkaConsumer
    {
        private readonly Queue<KafkaConsumedMessage> messages;
        private readonly IReadOnlyList<int> partitions;
        private readonly IReadOnlyDictionary<int, long> highWatermarks;

        public FakeKafkaConsumer(
            IReadOnlyList<int> partitions,
            IReadOnlyList<KafkaConsumedMessage> messages,
            IReadOnlyDictionary<int, long> highWatermarks)
        {
            this.partitions = partitions;
            this.messages = new Queue<KafkaConsumedMessage>(messages);
            this.highWatermarks = highWatermarks;
        }

        public string? AssignedTopic { get; private set; }

        public IReadOnlyList<int> AssignedPartitions { get; private set; } = [];

        public List<(string Topic, int Partition, long Offset)> SeekCalls { get; } = [];

        public IReadOnlyList<int> GetPartitions(string topic)
        {
            return this.partitions;
        }

        public void Assign(string topic, IReadOnlyCollection<int> partitionsToAssign)
        {
            this.AssignedTopic = topic;
            this.AssignedPartitions = partitionsToAssign.OrderBy(x => x).ToArray();
        }

        public void Seek(string topic, int partition, long offset)
        {
            this.SeekCalls.Add((topic, partition, offset));
        }

        public KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken)
        {
            if (this.messages.Count == 0)
            {
                return null;
            }

            return this.messages.Dequeue();
        }

        public long GetHighWatermarkOffset(string topic, int partition)
        {
            return this.highWatermarks[partition];
        }

        public void Close()
        {
        }

        public void Dispose()
        {
        }
    }
}
