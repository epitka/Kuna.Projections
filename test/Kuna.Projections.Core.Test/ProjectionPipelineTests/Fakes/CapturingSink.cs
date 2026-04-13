using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class CapturingSink : IProjectionStoreWriter<ItemModel>
{
    public List<PersistenceWriteBatch<ItemModel>> Batches { get; } = new();

    public Task<IReadOnlyList<PersistenceItemOutcome>> WriteBatch(
        PersistenceWriteBatch<ItemModel> batch,
        CancellationToken cancellationToken)
    {
        this.Batches.Add(batch);

        return Task.FromResult<IReadOnlyList<PersistenceItemOutcome>>(
            batch.Items
                 .Select(
                     item => new PersistenceItemOutcome(
                         item.Model.Id,
                         item.StageToken,
                         item.GlobalEventPosition,
                         PersistenceItemOutcomeStatus.Persisted,
                         null))
                 .ToArray());
    }
}
