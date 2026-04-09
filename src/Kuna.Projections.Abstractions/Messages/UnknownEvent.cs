using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Fallback event used when a stream contains an event type that is not mapped to
/// a registered CLR event for the current projection. This allows projections to
/// receive unsupported or irrelevant events through <c>Apply(UnknownEvent)</c>
/// and explicitly ignore or handle them instead of failing during deserialization.
/// This would be a case when processing unfiltered stream of events that contain events not relevant for the projection.
/// </summary>
/// <remarks>
/// Override <c>Apply(UnknownEvent)</c> in the projection to:
/// ignore event types that are not relevant for this projection,
/// tolerate forward-compatible events that may be introduced later, or
/// fail explicitly for unexpected event types that should not appear in the stream.
/// </remarks>
public class UnknownEvent : Event
{
    /// <summary>
    /// Gets the original event type name from the source message that could not be resolved.
    /// </summary>
    public required string UnknownEventName { get; init; }
}
