namespace Kuna.Projections.Core;

public interface IProjectionRuntimeStats
{
    ProjectionRuntimeStats ReadAndResetRuntimeStats();
}

public readonly record struct ProjectionRuntimeStats(
    long RuntimeProjectionHits,
    long ModelStateCacheRestores,
    long StoreProjectionLoads,
    long StoreProjectionMisses,
    long NewProjectionCreates);
