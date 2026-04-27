using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class InMemoryStateStoreSink
    : IModelStateSink<ItemModel>,
      IModelStateStore<ItemModel>
{
    private readonly Dictionary<Guid, ItemModel> models = new();

    public List<ModelStatesBatch<ItemModel>> Batches { get; } = new();

    public Task PersistBatch(ModelStatesBatch<ItemModel> batch, CancellationToken cancellationToken)
    {
        this.Batches.Add(batch);

        foreach (var change in batch.Changes)
        {
            if (change.ShouldDelete)
            {
                this.models.Remove(change.Model.Id);
                continue;
            }

            this.models[change.Model.Id] = Clone(change.Model);
        }

        return Task.CompletedTask;
    }

    public Task<ItemModel?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        return Task.FromResult(this.models.TryGetValue(modelId, out var model) ? Clone(model) : null);
    }

    private static ItemModel Clone(ItemModel model)
    {
        return new ItemModel
        {
            Id = model.Id,
            EventNumber = model.EventNumber,
            GlobalEventPosition = model.GlobalEventPosition,
            HasStreamProcessingFaulted = model.HasStreamProcessingFaulted,
            Name = model.Name,
        };
    }
}
