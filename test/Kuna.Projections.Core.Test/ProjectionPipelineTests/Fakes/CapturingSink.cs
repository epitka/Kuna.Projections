using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class CapturingSink : IModelStateSink<ItemModel>
{
    public List<ModelStatesBatch<ItemModel>> Batches { get; } = new();

    public Task PersistBatch(ModelStatesBatch<ItemModel> batch, CancellationToken cancellationToken)
    {
        this.Batches.Add(batch);
        return Task.CompletedTask;
    }
}
