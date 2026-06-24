using System.Collections.Concurrent;
using System.Reflection;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Resolves the projection model id for an EventSourcingDB event from the configured
/// authoritative source. Subjects are split on <c>/</c> and a configurable segment
/// (the last by default) is parsed as a <see cref="Guid"/>.
/// </summary>
public sealed class EventSourcingDbModelIdResolver : IEventModelIdResolver
{
    private readonly ConcurrentDictionary<Type, Func<Event, Guid?>?> accessors;
    private readonly ILogger logger;
    private readonly ModelIdResolutionStrategy strategy;
    private readonly int? subjectSegmentIndex;

    /// <summary>
    /// Initializes the resolver with the model-id resolution strategy and the subject
    /// segment index used when deriving the model id from the subject.
    /// </summary>
    public EventSourcingDbModelIdResolver(
        ILogger<EventSourcingDbModelIdResolver> logger,
        ModelIdResolutionStrategy strategy = ModelIdResolutionStrategy.UseModelIdAttribute,
        int? subjectSegmentIndex = null)
    {
        this.accessors = new ConcurrentDictionary<Type, Func<Event, Guid?>?>();
        this.logger = logger;
        this.strategy = strategy;
        this.subjectSegmentIndex = subjectSegmentIndex;
    }

    /// <summary>
    /// Attempts to resolve the projection model id for the given event and source
    /// subject. The <paramref name="streamId"/> parameter carries the EventSourcingDB
    /// subject.
    /// </summary>
    public bool TryResolve(Event @event, string streamId, out Guid modelId)
    {
        switch (this.strategy)
        {
            case ModelIdResolutionStrategy.UseModelIdAttribute:
                return this.TryResolveFromAttribute(@event, out modelId);
            case ModelIdResolutionStrategy.UseStreamId:
                return this.TryResolveFromSubject(streamId, out modelId);
            default:
                throw new ArgumentOutOfRangeException(
                    nameof(this.strategy),
                    this.strategy,
                    "Unsupported EventSourcingDB model id resolution strategy.");
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

    private bool TryResolveFromSubject(string subject, out Guid modelId)
    {
        modelId = Guid.Empty;

        if (string.IsNullOrWhiteSpace(subject))
        {
            return false;
        }

        try
        {
            var segments = subject.Split(
                '/',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            if (segments.Length == 0)
            {
                return false;
            }

            var segment = this.SelectSegment(segments);

            if (segment == null)
            {
                return false;
            }

            return Guid.TryParse(segment, out modelId);
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Could not parse subject {Subject}", subject);
            modelId = Guid.Empty;
            return false;
        }
    }

    private string? SelectSegment(string[] segments)
    {
        if (this.subjectSegmentIndex is null
            || this.subjectSegmentIndex < 0)
        {
            return segments[^1];
        }

        return this.subjectSegmentIndex.Value < segments.Length
                   ? segments[this.subjectSegmentIndex.Value]
                   : null;
    }
}
