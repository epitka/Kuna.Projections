namespace Kuna.Projections.Core;

internal interface IProjectionRuntimeStats
{
    ProjectionRuntimeStats ReadAndResetRuntimeStats();
}

internal readonly record struct ProjectionRuntimeStats(
    long RuntimeProjectionHits,
    long ModelStateCacheRestores,
    long StoreProjectionLoads,
    long StoreProjectionMisses,
    long NewProjectionCreates);
