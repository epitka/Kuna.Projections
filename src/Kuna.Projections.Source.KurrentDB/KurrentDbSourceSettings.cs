namespace Kuna.Projections.Source.KurrentDB;

/// <summary>
/// Source-specific settings for the KurrentDB event source implementation.
/// </summary>
public sealed class KurrentDbSourceSettings
{
    public const string SectionName = "KurrentDB";

    public KurrentDbFilterSettings Filter { get; init; } = new();
}
