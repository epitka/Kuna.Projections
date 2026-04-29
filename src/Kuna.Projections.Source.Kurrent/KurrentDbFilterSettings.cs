namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Declares the application-owned JSON shape that is translated into native
/// KurrentDB subscription filter objects.
/// </summary>
public sealed class KurrentDbFilterSettings
{
    public KurrentDbFilterKind Kind { get; init; } = KurrentDbFilterKind.StreamPrefix;

    public string[] Prefixes { get; init; } = [];

    public string Regex { get; init; } = string.Empty;
}
