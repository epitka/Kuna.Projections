namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Defines runtime settings that control projection batching, caching, and version checks.
/// </summary>
public interface IProjectionSettings<TState>
    where TState : class, Models.IModel, new()
{
    Models.PersistenceStrategy CatchUpPersistenceStrategy { get; set; }

    Models.PersistenceStrategy LiveProcessingPersistenceStrategy { get; set; }

    int MaxPendingProjectionsCount { get; set; }

    int SourceBufferCapacity { get; set; }

    int TransformSinkBufferCapacity { get; set; }

    ProjectionSourceKind Source { get; set; }

    ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; }

    int ReadBufferCapacity { get; set; }

    /// <summary>
    /// Flush delay in milliseconds.
    /// </summary>
    int LiveProcessingFlushDelay { get; set; }

    /// <summary>
    /// Skip failure generation when state is not found in the data store.
    /// </summary>
    bool SkipStateNotFoundFailure { get; set; }

    /// <summary>
    /// Minimum number of in-flight model cache entries retained even when pending batch sizes are small.
    /// </summary>
    int InFlightModelCacheMinEntries { get; set; }

    /// <summary>
    /// Dynamic capacity multiplier based on MaxPendingProjectionsCount.
    /// Effective cache size is max(InFlightModelCacheMinEntries, MaxPendingProjectionsCount * multiplier).
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
/// Default mutable implementation of <see cref="IProjectionSettings{TState}"/>.
/// </summary>
public class ProjectionSettings<TState> : IProjectionSettings<TState>
    where TState : class, Models.IModel, new()
{
    /// <summary>
    /// Persistence strategy used while the projection is catching up from an existing checkpoint.
    /// Use batching strategies here to improve replay throughput.
    /// </summary>
    public Models.PersistenceStrategy CatchUpPersistenceStrategy { get; set; } = Models.PersistenceStrategy.ModelCountBatching;

    /// <summary>
    /// Persistence strategy used after the projection reaches the live tail of the stream.
    /// The default favors lower latency over write batching.
    /// </summary>
    public Models.PersistenceStrategy LiveProcessingPersistenceStrategy { get; set; } = Models.PersistenceStrategy.ImmediateModelFlush;

    /// <summary>
    /// Maximum number of distinct models allowed to accumulate before a count-based flush is triggered.
    /// Increase this to batch more work per flush at the cost of memory and replay latency.
    /// </summary>
    public int MaxPendingProjectionsCount { get; set; } = 100;

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
    /// Delay in milliseconds before a time-based live flush is executed.
    /// Only applies to live strategies that batch writes over time.
    /// </summary>
    public int LiveProcessingFlushDelay { get; set; } = 1000;

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
    /// Multiplier used to scale in-flight cache capacity from <see cref="MaxPendingProjectionsCount"/>.
    /// Effective cache size is the greater of this derived value and the configured minimum entry count.
    /// </summary>
    public int InFlightModelCacheCapacityMultiplier { get; set; } = 3;

    /// <summary>
    /// Controls how strictly event version continuity is enforced while applying events to a model.
    /// Keep the default unless your event stream intentionally allows version gaps.
    /// </summary>
    public Models.EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = Models.EventVersionCheckStrategy.Consecutive;
}
