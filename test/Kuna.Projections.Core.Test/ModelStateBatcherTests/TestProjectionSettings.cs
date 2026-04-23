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

    public int SourceBufferCapacity { get; set; }

    public int TransformSinkBufferCapacity { get; set; } = 10000;

    public ProjectionSourceKind Source { get; set; } = ProjectionSourceKind.KurrentDB;

    public ModelIdResolutionStrategy ModelIdResolutionStrategy { get; set; } = ModelIdResolutionStrategy.PreferAttribute;

    public int ReadBufferCapacity { get; set; } = 12000;

    public bool SkipStateNotFoundFailure { get; set; } = true;

    public int InFlightModelCacheMinEntries { get; set; } = 10000;

    public int InFlightModelCacheCapacityMultiplier { get; set; } = 3;

    public EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = EventVersionCheckStrategy.Consecutive;
}
