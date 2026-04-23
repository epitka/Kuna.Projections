using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ModelStateBatcherTests;

internal sealed class TestProjectionSettings : IProjectionSettings<ItemModel>
{
    public ProjectionFlushSettings CatchUpFlush { get; set; } = new()
    {
        Strategy = PersistenceStrategy.ModelCountBatching,
    };

    public ProjectionFlushSettings LiveProcessingFlush { get; set; } = new()
    {
        Strategy = PersistenceStrategy.TimeBasedBatching,
    };

    public ProjectionBackpressureSettings Backpressure { get; set; } = new();

    public ProjectionSourceKind Source { get; set; } = ProjectionSourceKind.KurrentDB;

    public ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; } = ModelIdResolutionStrategy.PreferAttribute;

    public bool SkipStateNotFoundFailure { get; set; } = true;

    public int InFlightModelCacheMinEntries { get; set; } = 10000;

    public int InFlightModelCacheCapacityMultiplier { get; set; } = 3;

    public EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = EventVersionCheckStrategy.Consecutive;
}
