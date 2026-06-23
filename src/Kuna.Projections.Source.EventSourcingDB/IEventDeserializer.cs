using System.Text.Json;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Deserializes raw EventSourcingDB event payloads into projection event instances.
/// </summary>
public interface IEventDeserializer
{
    /// <summary>
    /// Deserializes one EventSourcingDB event payload into a projection event instance.
    /// </summary>
    Event Deserialize(JsonElement data, string eventTypeName, string globalEventId);
}

/// <summary>
/// Default <see cref="IEventDeserializer"/> for EventSourcingDB events. It resolves
/// the event CLR type from the CloudEvent <c>type</c> using a configurable name
/// resolver, deserializes the <c>data</c> payload, and returns
/// <see cref="UnknownEvent"/> when no registered type matches.
/// </summary>
/// <remarks>
/// Deserialization uses Newtonsoft.Json to match the other Kuna sources and because
/// the projection <see cref="Event"/> base type declares a <c>required</c> member
/// that System.Text.Json would otherwise enforce against payloads that do not carry it.
/// </remarks>
public class EventDeserializer : IEventDeserializer
{
    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    private readonly ILogger logger;
    private readonly Dictionary<string, Type> eventTypes;
    private readonly Func<string, string> eventTypeNameResolver;

    /// <summary>
    /// Initializes the deserializer with the known event CLR types that may appear
    /// in the source stream and an optional resolver that maps the CloudEvent
    /// <c>type</c> to the CLR type name used for lookup.
    /// </summary>
    public EventDeserializer(
        Type[] eventTypes,
        Func<string, string>? eventTypeNameResolver,
        ILogger<EventDeserializer> logger)
    {
        this.eventTypes = eventTypes.ToDictionary(x => x.Name, v => v, StringComparer.OrdinalIgnoreCase);
        this.eventTypeNameResolver = eventTypeNameResolver ?? DefaultEventTypeNameResolver;
        this.logger = logger;
    }

    /// <summary>
    /// Deserializes one EventSourcingDB <c>data</c> payload into the corresponding
    /// projection event type, or <see cref="UnknownEvent"/> when the resolved event
    /// name is not mapped to a registered CLR event type.
    /// </summary>
    public Event Deserialize(JsonElement data, string eventTypeName, string globalEventId)
    {
        var lookupName = this.eventTypeNameResolver(eventTypeName);

        this.eventTypes.TryGetValue(lookupName, out var eventType);

        if (eventType == null)
        {
            return new UnknownEvent
            {
                TypeName = nameof(UnknownEvent),
                UnknownEventName = eventTypeName,
            };
        }

        try
        {
            var @event = JsonConvert.DeserializeObject(data.GetRawText(), eventType, SerializerSettings) as Event
                         ?? throw new JsonSerializationException($"Deserialization of event {eventTypeName} ({globalEventId}) produced null.");

            @event.TypeName = eventType.Name;

            return @event;
        }
        catch (Exception ex)
        {
            this.logger.LogError(
                ex,
                "Could not deserialize event {EventName} at {GlobalEventId}",
                eventTypeName,
                globalEventId);

            throw;
        }
    }

    private static string DefaultEventTypeNameResolver(string eventType)
    {
        var separatorIndex = eventType.LastIndexOf('.');

        if (separatorIndex >= 0
            && separatorIndex < eventType.Length - 1)
        {
            return eventType[(separatorIndex + 1)..];
        }

        return eventType;
    }
}
