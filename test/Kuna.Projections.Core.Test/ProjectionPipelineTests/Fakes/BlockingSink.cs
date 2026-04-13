using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class BlockingSink : IProjectionStoreWriter<ItemModel>
{
    private readonly TaskCompletionSource<bool> firstPersistStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<bool> firstPersistRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int persistCalls;

    public TaskCompletionSource<bool> FirstPersistStarted => this.firstPersistStarted;

    public int PersistCalls => this.persistCalls;

    public async Task<IReadOnlyList<PersistenceItemOutcome>> WriteBatch(
        PersistenceWriteBatch<ItemModel> batch,
        CancellationToken cancellationToken)
    {
        var call = Interlocked.Increment(ref this.persistCalls);

        if (call == 1)
        {
            this.firstPersistStarted.TrySetResult(true);
            await this.firstPersistRelease.Task.WaitAsync(cancellationToken);
        }

        return batch.Items
                    .Select(
                        item => new PersistenceItemOutcome(
                            item.Model.Id,
                            item.StageToken,
                            item.GlobalEventPosition,
                            PersistenceItemOutcomeStatus.Persisted,
                            null))
                    .ToArray();
    }

    public void ReleaseFirstPersist()
    {
        this.firstPersistRelease.TrySetResult(true);
    }
}
