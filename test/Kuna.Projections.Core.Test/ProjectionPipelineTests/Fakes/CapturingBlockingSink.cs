using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class CapturingBlockingSink : IModelStateSink<ItemModel>
{
    private readonly TaskCompletionSource<bool> firstPersistStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource<bool> firstPersistRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public List<ModelStatesBatch<ItemModel>> Batches { get; } = new();

    public TaskCompletionSource<bool> FirstPersistStarted => this.firstPersistStarted;

    public async Task PersistBatch(ModelStatesBatch<ItemModel> batch, CancellationToken cancellationToken)
    {
        this.Batches.Add(batch);

        if (this.Batches.Count == 1)
        {
            this.firstPersistStarted.TrySetResult(true);
            await this.firstPersistRelease.Task.WaitAsync(cancellationToken);
        }
    }

    public void ReleaseFirstPersist()
    {
        this.firstPersistRelease.TrySetResult(true);
    }
}
