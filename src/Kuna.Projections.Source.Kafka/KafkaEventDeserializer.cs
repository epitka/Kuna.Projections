using System.Globalization;
using System.Text;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaEventDeserializer : IKafkaEventDeserializer
{
    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    private static readonly byte[] Utf8Bom = [0xEF, 0xBB, 0xBF,];
    private readonly Dictionary<string, Type> eventTypes;
    private readonly ILogger logger;

    public KafkaEventDeserializer(
        Type[] eventTypes,
        ILogger<KafkaEventDeserializer> logger)
    {
        this.eventTypes = eventTypes
                          .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
                          .ToDictionary(x => x.Key, x => x.First(), StringComparer.OrdinalIgnoreCase);

        this.logger = logger;
    }

    public Event Deserialize(byte[] eventData, string eventTypeName, long eventNumber)
    {
        this.eventTypes.TryGetValue(eventTypeName, out var eventType);

        if (eventType is null)
        {
            var unknownEvent = new UnknownEvent
            {
                TypeName = nameof(UnknownEvent),
                UnknownEventName = eventTypeName,
            };

            TryPopulateCreatedOn(eventData, unknownEvent);
            return unknownEvent;
        }

        try
        {
            var @event = JsonConvert.DeserializeObject(
                             Encoding.UTF8.GetString(eventData),
                             eventType,
                             SerializerSettings) as Event;

            @event!.TypeName = eventType.Name;
            return @event;
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Could not deserialize Kafka event {EventName} at {EventNumber}",
                eventTypeName,
                eventNumber);

            if (eventData.AsSpan().StartsWith(Utf8Bom))
            {
                this.logger.LogWarning(
                    "Kafka event {EventName} at {EventNumber} starts with BOM, retrying without BOM",
                    eventTypeName,
                    eventNumber);

                return this.Deserialize(eventData[Utf8Bom.Length..], eventTypeName, eventNumber);
            }

            throw;
        }
    }

    private static void TryPopulateCreatedOn(
        byte[] eventData,
        UnknownEvent unknownEvent)
    {
        try
        {
            var obj = JObject.Parse(Encoding.UTF8.GetString(eventData));

            if (obj.TryGetValue(nameof(Event.CreatedOn), StringComparison.OrdinalIgnoreCase, out var createdOnToken))
            {
                var createdOnRaw = createdOnToken?.ToString();

                if (!string.IsNullOrWhiteSpace(createdOnRaw)
                    && DateTime.TryParse(
                        createdOnRaw,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.RoundtripKind,
                        out var createdOn))
                {
                    unknownEvent.CreatedOn = createdOn;
                }
            }
        }
        catch
        {
            // Ignore malformed payloads and leave UnknownEvent with default CreatedOn.
        }
    }
}
