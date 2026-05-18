using System.Globalization;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record KafkaWritePlan(
    string Topic,
    IReadOnlyList<InterleavedOrderEvent> Events);

public static class KafkaStreamWriter
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    public static async Task WriteAsync(
        KafkaWritePlan plan,
        string bootstrapServers,
        CancellationToken cancellationToken = default)
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                BrokerAddressFamily = BrokerAddressFamily.V4,
            }).Build();

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var written = 0;

        foreach (var item in plan.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var createdOn = item.Event.CreatedOn;
            var modelId = ResolveModelId(item.StreamName);
            var payload = SerializePayload(item, createdOn);

            await producer.ProduceAsync(
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
                cancellationToken);

            written++;
        }

        producer.Flush(TimeSpan.FromSeconds(30));
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
