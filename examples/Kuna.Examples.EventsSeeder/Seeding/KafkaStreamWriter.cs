using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Kuna.StreamGenerator;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record KafkaWritePlan(
    string Topic,
    IReadOnlyList<InterleavedOrderEvent> Events);

public static class KafkaStreamWriter
{
    private const int MaxInFlightProduceOperations = 2048;

    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    public static async Task WriteAsync(
        KafkaWritePlan plan,
        string bootstrapServers,
        IProgress<StreamWriteProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                BrokerAddressFamily = BrokerAddressFamily.V4,
                EnableIdempotence = true,
                Acks = Acks.All,
                LingerMs = 20,
                BatchSize = 256 * 1024,
                CompressionType = CompressionType.Lz4,
                QueueBufferingMaxMessages = 200_000,
            }).Build();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var written = 0;
        var inFlight = new List<Task>(MaxInFlightProduceOperations);

        foreach (var item in plan.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var createdOn = item.Event.CreatedOn;
            var modelId = ResolveModelId(item.StreamName);
            var payload = SerializePayload(item, createdOn);

            inFlight.Add(
                producer.ProduceAsync(
                    plan.Topic,
                    new Message<string, byte[]>
                    {
                        Key = modelId.ToString("D"),
                        Value = payload,
                        Timestamp = new Timestamp(createdOn),
                        Headers =
                        [
                            new Header("event-type", Encoding.UTF8.GetBytes(item.EventTypeName)),
                            new Header("event-number", Encoding.UTF8.GetBytes(item.SequenceInStream.ToString(CultureInfo.InvariantCulture))),
                            new Header("created-on", Encoding.UTF8.GetBytes(createdOn.ToString("O", CultureInfo.InvariantCulture))),
                            new Header("stream-id", Encoding.UTF8.GetBytes(item.StreamName)),
                        ],
                    },
                    cancellationToken));

            if (inFlight.Count < MaxInFlightProduceOperations)
            {
                continue;
            }

            written = await DrainCompletedProduceAsync(inFlight, written, plan.Events.Count, stopwatch, progress);
        }

        while (inFlight.Count > 0)
        {
            written = await DrainCompletedProduceAsync(inFlight, written, plan.Events.Count, stopwatch, progress);
        }

        producer.Flush(TimeSpan.FromSeconds(30));
    }

    private static async Task<int> DrainCompletedProduceAsync(
        List<Task> inFlight,
        int written,
        int totalEvents,
        System.Diagnostics.Stopwatch stopwatch,
        IProgress<StreamWriteProgress>? progress)
    {
        var completed = await Task.WhenAny(inFlight);
        await completed;
        _ = inFlight.Remove(completed);

        written++;

        progress?.Report(
            new StreamWriteProgress(
                written,
                totalEvents,
                stopwatch.Elapsed));

        return written;
    }

    private static Guid ResolveModelId(string streamName)
    {
        var separatorIndex = streamName.IndexOf('-', StringComparison.Ordinal);

        if (separatorIndex >= 0
            && separatorIndex < streamName.Length - 1
            && Guid.TryParse(streamName[(separatorIndex + 1)..], out var modelId))
        {
            return modelId;
        }

        throw new InvalidOperationException($"Stream '{streamName}' does not contain a Guid model id suffix.");
    }

    private static byte[] SerializePayload(InterleavedOrderEvent item, DateTime createdOn)
    {
        item.Event.TypeName = string.IsNullOrWhiteSpace(item.Event.TypeName) ? item.Event.GetType().Name : item.Event.TypeName;
        item.Event.CreatedOn = createdOn;

        return JsonSerializer.SerializeToUtf8Bytes(item.Event, item.Event.GetType(), JsonSerializerOptions);
    }
}
