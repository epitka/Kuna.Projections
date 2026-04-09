using Akka;
using Akka.Streams.Dsl;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Core;

/// <summary>
/// Groups transformed model states into sink-ready batches according to the
/// configured persistence strategy. It also collapses multiple changes for the
/// same model within a batch down to the last state that should be persisted.
/// </summary>
internal static class ModelStateBatcher
{
    public static Flow<ModelState<TState>, ModelStatesBatch<TState>, NotUsed> Create<TState>(IProjectionSettings<TState> settings)
        where TState : class, IModel, new()
    {
        switch (settings.CatchUpPersistenceStrategy)
        {
            case PersistenceStrategy.ImmediateModelFlush:
                return Flow.Create<ModelState<TState>>()
                           .Select(change => ToBatch([change,]));

            case PersistenceStrategy.TimeBasedBatching:
                return Flow.Create<ModelState<TState>>()
                           .GroupedWithin(
                               Math.Max(1, settings.MaxPendingProjectionsCount),
                               TimeSpan.FromMilliseconds(Math.Max(1, settings.LiveProcessingFlushDelay)))
                           .Select(ToBatch);

            case PersistenceStrategy.ModelCountBatching:
            default:
                return Flow.Create<ModelState<TState>>()
                           .Grouped(Math.Max(1, settings.MaxPendingProjectionsCount))
                           .Select(ToBatch);
        }
    }

    private static ModelStatesBatch<TState> ToBatch<TState>(IEnumerable<ModelState<TState>> changes)
        where TState : class, IModel, new()
    {
        var lastPosition = default(GlobalEventPosition);
        var map = new Dictionary<Guid, ModelState<TState>>();

        foreach (var change in changes)
        {
            if (change is { IsNew: true, ShouldDelete: true, })
            {
                continue;
            }

            map[change.Model.Id] = change;
            lastPosition = change.GlobalEventPosition;
        }

        return new ModelStatesBatch<TState>
        {
            Changes = map.Values.ToList(),
            GlobalEventPosition = lastPosition,
        };
    }
}
