namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Selects which native KurrentDB filter shape should be built.
/// </summary>
public enum KurrentDbFilterKind
{
    StreamPrefix = 0,
    StreamRegex = 1,
    EventTypePrefix = 2,
    EventTypeRegex = 3,
}
