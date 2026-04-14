using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class MixedOutcomeSink : IProjectionStoreWriter<ItemModel>
{
    private readonly Guid failedModelId;

    public MixedOutcomeSink(Guid failedModelId)
    {
        this.failedModelId = failedModelId;
    }

    public List<PersistenceWriteBatch<ItemModel>> Batches { get; } = new();

    public Task<IReadOnlyList<PersistenceItemOutcome>> WriteBatch(
        PersistenceWriteBatch<ItemModel> batch,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        this.Batches.Add(batch);

        var outcomes = batch.Items
                            .Select(
                                item => new PersistenceItemOutcome(
                                    item.Model.Id,
                                    item.StagedVersionToken,
                                    item.GlobalEventPosition,
                                    this.Batches.Count == 1 && item.Model.Id == this.failedModelId
                                        ? PersistenceItemOutcomeStatus.Failed
                                        : PersistenceItemOutcomeStatus.Persisted,
                                    null))
                            .ToArray();

        return Task.FromResult<IReadOnlyList<PersistenceItemOutcome>>(outcomes);
    }
}
