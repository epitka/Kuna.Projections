namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Defines runtime settings that control projection batching, caching, and version checks.
/// </summary>
public interface IProjectionSettings<TState>
    where TState : class, Models.IModel, new()
{
    ProjectionFlushSettings CatchUpFlush { get; set; }

    ProjectionFlushSettings LiveProcessingFlush { get; set; }

    int SourceBufferCapacity { get; set; }

    int TransformSinkBufferCapacity { get; set; }

    ProjectionSourceKind Source { get; set; }

    ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; }

    int ReadBufferCapacity { get; set; }

    /// <summary>
    /// Skip failure generation when state is not found in the data store.
    /// </summary>
    bool SkipStateNotFoundFailure { get; set; }

    /// <summary>
    /// Minimum number of in-flight model cache entries retained even when pending batch sizes are small.
    /// </summary>
    int InFlightModelCacheMinEntries { get; set; }

    /// <summary>
    /// Dynamic capacity multiplier based on model-count flush thresholds.
    /// Effective cache size is max(InFlightModelCacheMinEntries, max(CatchUpFlush.ModelCountThreshold, LiveProcessingFlush.ModelCountThreshold) * multiplier).
    /// </summary>
    int InFlightModelCacheCapacityMultiplier { get; set; }

    Models.EventVersionCheckStrategy EventVersionCheckStrategy { get; set; }
}

/// <summary>
/// Shared section-name constants for projection settings.
/// </summary>
public static class ProjectionSettingsSection
{
    public const string Name = "Projections";
}

/// <summary>
/// Defines flush behavior for a projection processing phase.
/// </summary>
public class ProjectionFlushSettings
{
    /// <summary>
    /// Persistence strategy used by the phase.
    /// </summary>
    public Models.PersistenceStrategy Strategy { get; set; } = Models.PersistenceStrategy.ModelCountBatching;

    /// <summary>
    /// Number of distinct models allowed to accumulate before a count-based flush is triggered.
    /// Applies when <see cref="Strategy"/> is <see cref="Models.PersistenceStrategy.ModelCountBatching"/>.
    /// </summary>
    public int ModelCountThreshold { get; set; } = 100;

    /// <summary>
    /// Delay in milliseconds before a time-based flush is triggered.
    /// Applies when <see cref="Strategy"/> is <see cref="Models.PersistenceStrategy.TimeBasedBatching"/>.
    /// </summary>
    public int Delay { get; set; } = 1000;
}

/// <summary>
/// Default mutable implementation of <see cref="IProjectionSettings{TState}"/>.
/// </summary>
public class ProjectionSettings<TState> : IProjectionSettings<TState>
    where TState : class, Models.IModel, new()
{
    /// <summary>
    /// Flush behavior used while the projection is catching up from an existing checkpoint.
    /// </summary>
    public ProjectionFlushSettings CatchUpFlush { get; set; } = new()
    {
        Strategy = Models.PersistenceStrategy.ModelCountBatching,
        ModelCountThreshold = 100,
        Delay = 1000,
    };

    /// <summary>
    /// Flush behavior used after the projection reaches the live tail of the stream.
    /// </summary>
    public ProjectionFlushSettings LiveProcessingFlush { get; set; } = new()
    {
        Strategy = Models.PersistenceStrategy.ImmediateModelFlush,
        ModelCountThreshold = 100,
        Delay = 1000,
    };

    /// <summary>
    /// Number of source envelopes that may be buffered ahead of transformation.
    /// Increase this only when the source is starved by backpressure and you have memory headroom.
    /// </summary>
    public int SourceBufferCapacity { get; set; } = 10000;

    /// <summary>
    /// Number of transformed signals that may be buffered ahead of sink batching
    /// and persistence. Keep this much smaller than source buffering when the
    /// source can replay faster than the sink can persist.
    /// </summary>
    public int TransformSinkBufferCapacity { get; set; } = 10000;

    /// <summary>
    /// Selects which event source implementation should be used for the projection.
    /// </summary>
    public ProjectionSourceKind Source { get; set; } = ProjectionSourceKind.KurrentDB;

    /// <summary>
    /// Controls how the target model id is resolved from incoming events.
    /// </summary>
    public ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; } = ModelIdResolutionStrategy.PreferAttribute;

    /// <summary>
    /// Number of envelopes that may be buffered between the source subscription task and the async consumer.
    /// This is an application-side runtime buffer, not a KurrentDB connection setting.
    /// </summary>
    public int ReadBufferCapacity { get; set; } = 12000;

    /// <summary>
    /// When true, missing state for a non-initial event is ignored instead of recorded as a projection failure.
    /// Keep this false unless your stream intentionally tolerates missing historical state.
    /// </summary>
    public bool SkipStateNotFoundFailure { get; set; } = false;

    /// <summary>
    /// Minimum in-flight cache capacity retained even when pending batch sizes are small.
    /// Raise this if cache churn is high at lower batch counts.
    /// </summary>
    public int InFlightModelCacheMinEntries { get; set; } = 10000;

    /// <summary>
    /// Multiplier used to scale in-flight cache capacity from configured model-count flush thresholds.
    /// Effective cache size is the greater of this derived value and the configured minimum entry count.
    /// </summary>
    public int InFlightModelCacheCapacityMultiplier { get; set; } = 3;

    /// <summary>
    /// Controls how strictly event version continuity is enforced while applying events to a model.
    /// Keep the default unless your event stream intentionally allows version gaps.
    /// </summary>
    public Models.EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = Models.EventVersionCheckStrategy.Consecutive;
}
