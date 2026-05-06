using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ModelStateBatcherTests;

internal sealed class TestProjectionSettings : IProjectionSettings<ItemModel>
{
    public string InstanceId { get; set; } = "test-instance";

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

    public int ModelStateCacheCapacity { get; set; } = 10000;

    public EventVersionCheckStrategy EventVersionCheckStrategy { get; set; } = EventVersionCheckStrategy.Consecutive;
}
