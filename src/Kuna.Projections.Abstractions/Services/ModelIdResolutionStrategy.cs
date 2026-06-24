namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Determines how a source resolves a projection model id.
/// </summary>
public enum ModelIdResolutionStrategy
{
    UseModelIdAttribute = 0,
    UseStreamId = 1,
}
