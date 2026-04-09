namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Settings that control how the Kurrent event source feeds events into the
/// projection pipeline.
/// </summary>
public class EventStoreSourceSettings
{
    public const string SectionName = "EventStoreSource";

    public string StreamName { get; set; } = string.Empty;

    public int EventsBoundedCapacity { get; init; } = 12000;

    public ModelIdResolutionStrategy ModelIdResolutionStrategy { get; init; } = ModelIdResolutionStrategy.PreferAttribute;
}
