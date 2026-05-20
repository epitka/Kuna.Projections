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
                ConsumerGroupId = "orders-consumer",
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
        consumer.AssignedStartOffsets[0].ShouldBe(5);
        consumer.SeekCalls.ShouldBeEmpty();
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
                ConsumerGroupId = "orders-consumer",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<ProjectionCaughtUpEvent>();
    }

    [Fact]
    public async Task ReadAll_Should_Reemit_CaughtUp_After_Live_Records_Arrive()
    {
        var modelId = Guid.NewGuid();
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages: [],
            highWatermarks: new Dictionary<int, long> { [0] = 0, });

        var source = CreateSource(
            consumer,
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                ConsumerGroupId = "orders-consumer",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<ProjectionCaughtUpEvent>();

        consumer.Enqueue(
            new KafkaConsumedMessage
            {
                Topic = "orders-events",
                Partition = 0,
                Offset = 0,
                KeyBytes = System.Text.Encoding.UTF8.GetBytes(modelId.ToString("D")),
                ValueBytes = System.Text.Encoding.UTF8.GetBytes("""{"value":"live"}"""),
                Headers = new Dictionary<string, byte[]>
                {
                    ["event-type"] = System.Text.Encoding.UTF8.GetBytes(nameof(TestEvent)),
                    ["event-number"] = System.Text.Encoding.UTF8.GetBytes("1"),
                    ["created-on"] = System.Text.Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                },
            });

        consumer.SetHighWatermark(partition: 0, highWatermark: 1);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<TestEvent>().Value.ShouldBe("live");

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<ProjectionCaughtUpEvent>();
    }

    [Fact]
    public async Task ReadAll_Should_Not_Seek_When_No_Checkpoint_Exists()
    {
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages:
            [
                new KafkaConsumedMessage
                {
                    Topic = "orders-events",
                    Partition = 0,
                    Offset = 0,
                    KeyBytes = System.Text.Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("D")),
                    ValueBytes = System.Text.Encoding.UTF8.GetBytes("""{"value":"abc"}"""),
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["event-type"] = System.Text.Encoding.UTF8.GetBytes(nameof(TestEvent)),
                        ["event-number"] = System.Text.Encoding.UTF8.GetBytes("1"),
                        ["created-on"] = System.Text.Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                    },
                },
            ],
            highWatermarks: new Dictionary<int, long> { [0] = 1, });

        var source = CreateSource(
            consumer,
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                ConsumerGroupId = "orders-consumer",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            });

        using var cts = new CancellationTokenSource();
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        consumer.SeekCalls.ShouldBeEmpty();
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
                ConsumerGroupId = "orders-consumer",
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

    [Fact]
    public async Task ReadAll_Should_Throw_When_Configured_Partition_Does_Not_Exist()
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
                ConsumerGroupId = "orders-consumer",
                Topic = "orders-events",
                Partitions = [1,],
                PollTimeoutMs = 1,
            });

        var ex = await Should.ThrowAsync<InvalidOperationException>(
                     async () =>
                     {
                         await foreach (var _ in source.ReadAll(new GlobalEventPosition(string.Empty), CancellationToken.None))
                         {
                         }
                     });

        ex.Message.ShouldContain("does not contain configured partitions");
    }

    [Fact]
    public async Task ReadAll_Should_Use_Configured_Consumer_Group_Id()
    {
        var consumer = new FakeKafkaConsumer(
            partitions: [0,],
            messages: [],
            highWatermarks: new Dictionary<int, long> { [0] = 0, });

        var consumerFactory = new FakeKafkaConsumerFactory(consumer);
        var source = new KafkaEventSource<TestModel>(
            consumerFactory,
            new KunaKafkaSourceTransformer(),
            new KafkaEventEnvelopeFactory(new KafkaEventDeserializer([typeof(TestEvent),], NullLogger<KafkaEventDeserializer>.Instance)),
            new KafkaCheckpointSerializer(),
            new KafkaSourceSettings
            {
                BootstrapServers = "localhost:9092",
                ConsumerGroupId = "orders-consumer",
                Topic = "orders-events",
                PollTimeoutMs = 1,
            },
            new ProjectionSettings<TestModel>
            {
                InstanceId = "orders-v1",
            },
            NullLogger<KafkaEventSource<TestModel>>.Instance);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(string.Empty), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        consumerFactory.LastRequestedConsumerGroup.ShouldBe("orders-consumer");
    }

    private static KafkaEventSource<TestModel> CreateSource(
        IKafkaConsumer consumer,
        KafkaSourceSettings sourceSettings)
    {
        return new KafkaEventSource<TestModel>(
            new FakeKafkaConsumerFactory(consumer),
            new KunaKafkaSourceTransformer(),
            new KafkaEventEnvelopeFactory(new KafkaEventDeserializer([typeof(TestEvent),], NullLogger<KafkaEventDeserializer>.Instance)),
            new KafkaCheckpointSerializer(),
            sourceSettings,
            new ProjectionSettings<TestModel>
            {
                InstanceId = "orders-v1",
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

        public string? LastRequestedConsumerGroup { get; private set; }

        public IKafkaConsumer Create(KafkaSourceSettings sourceSettings, string consumerGroupId)
        {
            this.LastRequestedConsumerGroup = consumerGroupId;
            return this.consumer;
        }
    }

    private sealed class FakeKafkaConsumer : IKafkaConsumer
    {
        private readonly Queue<KafkaConsumedMessage> messages;
        private readonly IReadOnlyList<int> partitions;
        private readonly Dictionary<int, long> highWatermarks;

        public FakeKafkaConsumer(
            IReadOnlyList<int> partitions,
            IReadOnlyList<KafkaConsumedMessage> messages,
            IReadOnlyDictionary<int, long> highWatermarks)
        {
            this.partitions = partitions;
            this.messages = new Queue<KafkaConsumedMessage>(messages);
            this.highWatermarks = new Dictionary<int, long>(highWatermarks);
        }

        public string? AssignedTopic { get; private set; }

        public IReadOnlyList<int> AssignedPartitions { get; private set; } = [];

        public IReadOnlyDictionary<int, long> AssignedStartOffsets { get; private set; } = new Dictionary<int, long>();

        public List<(string Topic, int Partition, long Offset)> SeekCalls { get; } = [];

        public void Enqueue(KafkaConsumedMessage message)
        {
            this.messages.Enqueue(message);
        }

        public void SetHighWatermark(int partition, long highWatermark)
        {
            this.highWatermarks[partition] = highWatermark;
        }

        public IReadOnlyList<int> GetPartitions(string topic)
        {
            return this.partitions;
        }

        public void Assign(string topic, IReadOnlyCollection<int> partitionsToAssign, IReadOnlyDictionary<int, long>? startOffsets = null)
        {
            this.AssignedTopic = topic;
            this.AssignedPartitions = partitionsToAssign.OrderBy(x => x).ToArray();
            this.AssignedStartOffsets = startOffsets is null
                                            ? new Dictionary<int, long>()
                                            : new Dictionary<int, long>(startOffsets);
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
