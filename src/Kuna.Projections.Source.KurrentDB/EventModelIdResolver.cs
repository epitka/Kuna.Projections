using System.Collections.Concurrent;
using System.Reflection;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.KurrentDB;

/// <summary>
/// Resolves the projection model id for an event from the configured authoritative source.
/// </summary>
public sealed class EventModelIdResolver : IEventModelIdResolver
{
    private readonly ConcurrentDictionary<Type, Func<Event, Guid?>?> accessors;
    private readonly ILogger logger;
    private readonly ModelIdResolutionStrategy strategy;

    /// <summary>
    /// Initializes the resolver with the specified model-id resolution strategy.
    /// </summary>
    public EventModelIdResolver(
        ILogger<EventModelIdResolver> logger,
        ModelIdResolutionStrategy strategy = ModelIdResolutionStrategy.UseModelIdAttribute)
    {
        this.accessors = new ConcurrentDictionary<Type, Func<Event, Guid?>?>();
        this.logger = logger;
        this.strategy = strategy;
    }

    /// <summary>
    /// Attempts to resolve the projection model id for the given event and
    /// source stream id.
    /// </summary>
    public bool TryResolve(Event @event, string streamId, out Guid modelId)
    {
        switch (this.strategy)
        {
            case ModelIdResolutionStrategy.UseModelIdAttribute:
                return this.TryResolveFromAttribute(@event, out modelId);
            case ModelIdResolutionStrategy.UseStreamId:
                return this.TryResolveFromStreamId(streamId, out modelId);
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(this.strategy),
                    this.strategy,
                    "Unsupported KurrentDB model id resolution strategy.");
        }
    }

    private bool TryResolveFromAttribute(Event @event, out Guid modelId)
    {
        var accessor = this.accessors.GetOrAdd(@event.GetType(), this.CreateAccessor);

        if (accessor == null)
        {
            modelId = Guid.Empty;
            return false;
        }

        var value = accessor(@event);

        if (!value.HasValue
            || value.Value == Guid.Empty)
        {
            modelId = Guid.Empty;
            return false;
        }

        modelId = value.Value;
        return true;
    }

    private Func<Event, Guid?>? CreateAccessor(Type eventType)
    {
        var properties = eventType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                  .Where(p => p.GetCustomAttribute<ModelIdAttribute>() != null)
                                  .ToArray();

        switch (properties.Length)
        {
            case 0:
                return null;
            case > 1:
                this.logger.LogWarning(
                    "Multiple [ModelId] properties found on {EventType}, using {PropertyName}",
                    eventType.Name,
                    properties[0].Name);

                break;
        }

        var property = properties[0];

        if (property.GetMethod == null)
        {
            this.logger.LogWarning(
                "[ModelId] property {PropertyName} on {EventType} has no getter",
                property.Name,
                eventType.Name);

            return null;
        }

        if (property.PropertyType == typeof(Guid))
        {
            return @event => (Guid)property.GetValue(@event)!;
        }

        if (property.PropertyType == typeof(Guid?))
        {
            return @event => (Guid?)property.GetValue(@event);
        }

        if (property.PropertyType == typeof(string))
        {
            return @event =>
            {
                var raw = property.GetValue(@event) as string;

                if (string.IsNullOrWhiteSpace(raw))
                {
                    return null;
                }

                return Guid.TryParse(raw, out var parsed) ? parsed : null;
            };
        }

        this.logger.LogWarning(
            "[ModelId] property {PropertyName} on {EventType} has unsupported type {PropertyType}",
            property.Name,
            eventType.Name,
            property.PropertyType.Name);

        return null;
    }

    private bool TryResolveFromStreamId(string streamId, out Guid modelId)
    {
        try
        {
            var separatorIndex = streamId.IndexOf('-');

            if (separatorIndex < 0
                || separatorIndex == streamId.Length - 1)
            {
                modelId = Guid.Empty;
                return false;
            }

            var rawModelId = streamId[(separatorIndex + 1)..];

            if (Guid.TryParse(rawModelId, out modelId))
            {
                return true;
            }

            modelId = Guid.Empty;
            return false;
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Could not parse streamId {Stream}", streamId);
            modelId = Guid.Empty;
            return false;
        }
    }
}
