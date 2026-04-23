namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Source-specific settings for the KurrentDB event source implementation.
/// </summary>
public sealed class KurrentDbSourceSettings
{
    public const string SectionName = "KurrentDB";

    public int SubscriptionBufferCapacity { get; init; } = 12000;

    public KurrentDbFilterSettings Filter { get; init; } = new();
}
