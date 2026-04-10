namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Determines how a source resolves a projection model id when it may be
/// available from both event data and stream id.
/// </summary>
public enum ModelIdResolutionStrategy
{
    PreferAttribute = 0,
    RequireStreamId = 1,
    RequireMatch = 2,
}
