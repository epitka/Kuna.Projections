namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Defines runtime settings that control projection batching, caching, and version checks.
/// </summary>
public interface IProjectionSettings<TState>
    where TState : class, Models.IModel, new()
{
    ProjectionFlushSettings CatchUpFlush { get; set; }

    ProjectionFlushSettings LiveProcessingFlush { get; set; }

    ProjectionBackpressureSettings Backpressure { get; set; }

    ProjectionSourceKind Source { get; set; }

    ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; }

    /// <summary>
    /// Number of model states retained in memory for fast reloads after runtime projection state is cleared.
    /// </summary>
    int ModelStateCacheCapacity { get; set; }

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
/// Defines backpressure buffer capacities between projection pipeline stages.
/// </summary>
public class ProjectionBackpressureSettings
{
    /// <summary>
    /// Number of source envelopes that may be buffered ahead of transformation.
    /// Increase this only when the source is starved by backpressure and you have memory headroom.
    /// </summary>
    public int SourceToTransformBufferCapacity { get; set; } = 10000;

    /// <summary>
    /// Number of transformed signals that may be buffered ahead of sink batching and persistence.
    /// </summary>
    public int TransformToSinkBufferCapacity { get; set; } = 10000;
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
    /// Backpressure buffer capacities between projection pipeline stages.
    /// </summary>
    public ProjectionBackpressureSettings Backpressure { get; set; } = new();

    /// <summary>
    /// Selects which event source implementation should be used for the projection.
    /// </summary>
    public ProjectionSourceKind Source { get; set; } = ProjectionSourceKind.KurrentDB;

    /// <summary>
    /// Controls how the target model id is resolved from incoming events.
    /// </summary>
    public ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; } = ModelIdResolutionStrategy.PreferAttribute;

    /// <summary>
    /// Number of model states retained in memory for fast reloads after runtime projection state is cleared.
    /// </summary>
    public int ModelStateCacheCapacity { get; set; } = 10000;

    /// <summary>
    /// Controls how strictly event version continuity is enforced while applying events to a model.
    /// Keep the default unless your event stream intentionally allows version gaps.
    /// </summary>
    public Models.EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = Models.EventVersionCheckStrategy.Consecutive;
}
