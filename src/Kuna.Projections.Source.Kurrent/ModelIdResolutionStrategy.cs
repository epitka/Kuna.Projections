namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Determines how the Kurrent source resolves a projection model id when it is
/// available from both event data and stream id.
/// </summary>
public enum ModelIdResolutionStrategy
{
    /// <summary>
    /// Uses the model id from the event's <c>[ModelId]</c> property when
    /// available, and falls back to the stream id when the event does not
    /// expose one.
    /// </summary>
    PreferAttribute = 0,

    /// <summary>
    /// Requires the stream id to contain the model id. When the event also
    /// exposes a <c>[ModelId]</c> property, the stream id still remains the
    /// authoritative source.
    /// </summary>
    RequireStreamId = 1,

    /// <summary>
    /// Requires both the stream id and the event's <c>[ModelId]</c> property to
    /// resolve a model id, and only succeeds when both values are present and
    /// equal.
    /// </summary>
    RequireMatch = 2,
}
