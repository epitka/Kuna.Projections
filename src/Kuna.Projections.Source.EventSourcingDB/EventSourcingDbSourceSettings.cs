namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Source-specific settings for the EventSourcingDB event source implementation.
/// </summary>
public sealed class EventSourcingDbSourceSettings
{
    public const string SectionName = "EventSourcingDB";

    /// <summary>
    /// Subject the source observes. Defaults to the root subject so that, together
    /// with <see cref="Recursive"/>, the source receives the full, globally ordered
    /// event stream.
    /// </summary>
    public string Subject { get; init; } = "/";

    /// <summary>
    /// Whether nested subjects below <see cref="Subject"/> are included. Defaults to
    /// <see langword="true"/>.
    /// </summary>
    public bool Recursive { get; init; } = true;

    /// <summary>
    /// Zero-based index of the subject path segment used as the model-id candidate
    /// when resolving the model id from the subject. <see langword="null"/> (the
    /// default) selects the last non-empty segment, e.g. <c>/orders/{guid}</c> uses
    /// <c>{guid}</c>.
    /// </summary>
    public int? ModelIdSubjectSegmentIndex { get; init; }
}
