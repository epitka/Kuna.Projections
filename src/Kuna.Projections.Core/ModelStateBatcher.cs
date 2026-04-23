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
        switch (settings.CatchUpFlush.Strategy)
        {
            case PersistenceStrategy.ImmediateModelFlush:
                return Flow.Create<ModelState<TState>>()
                           .Select(change => ToBatch([change,]));

            case PersistenceStrategy.TimeBasedBatching:
                return Flow.Create<ModelState<TState>>()
                           .GroupedWithin(
                               int.MaxValue,
                               NormalizeDelay(settings.CatchUpFlush.Delay))
                           .Select(ToBatch);

            case PersistenceStrategy.ModelCountBatching:
            default:
                return Flow.Create<ModelState<TState>>()
                           .Select(BatchInput<TState>.ForChange)
                           .Concat(Source.Single(BatchInput<TState>.Complete()))
                           .StatefulSelectMany(() => CreateDistinctModelBatcher<TState>(Math.Max(1, settings.CatchUpFlush.ModelCountThreshold)));
        }
    }

    private static TimeSpan NormalizeDelay(int delayMilliseconds)
    {
        return TimeSpan.FromMilliseconds(Math.Max(1, delayMilliseconds));
    }

    private static Func<BatchInput<TState>, IEnumerable<ModelStatesBatch<TState>>> CreateDistinctModelBatcher<TState>(int maxModelCount)
        where TState : class, IModel, new()
    {
        var changesByModel = new Dictionary<Guid, ModelState<TState>>();
        var lastPosition = default(GlobalEventPosition);
        var sawInput = false;

        return input =>
        {
            if (input.IsComplete)
            {
                return !sawInput
                           ? []
                           : [Flush(),];
            }

            var change = input.Change
                         ?? throw new InvalidOperationException("Batch input must contain a change unless it is complete.");

            sawInput = true;

            if (change is not { IsNew: true, ShouldDelete: true, })
            {
                changesByModel[change.Model.Id] = change;
                lastPosition = change.GlobalEventPosition;
            }

            return changesByModel.Count >= maxModelCount
                       ? [Flush(),]
                       : [];

            ModelStatesBatch<TState> Flush()
            {
                var batch = new ModelStatesBatch<TState>
                {
                    Changes = changesByModel.Values.ToList(),
                    GlobalEventPosition = lastPosition,
                };

                changesByModel.Clear();
                lastPosition = default;
                sawInput = false;

                return batch;
            }
        };
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

    private readonly record struct BatchInput<TState>(ModelState<TState>? Change, bool IsComplete)
        where TState : class, IModel, new()
    {
        public static BatchInput<TState> ForChange(ModelState<TState> change)
        {
            return new BatchInput<TState>(change, false);
        }

        public static BatchInput<TState> Complete()
        {
            return new BatchInput<TState>(default, true);
        }
    }
}
