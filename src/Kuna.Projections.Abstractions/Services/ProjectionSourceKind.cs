namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Selects which event source implementation a projection uses.
/// </summary>
public enum ProjectionSourceKind
{
    Unspecified = 0,
    KurrentDB = 1,
    Kafka = 2,
}
