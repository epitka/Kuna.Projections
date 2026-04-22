using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class CountingEngineLike
    : IModelStateTransformer<EventEnvelope, ItemModel>,
      IProjectionLifecycle
{
    private readonly List<Guid> clearedModelIds = new();
    private int transformedCount;

    public int TransformedCount => this.transformedCount;

    public IReadOnlyList<Guid> ClearedModelIds => this.clearedModelIds;

    public ValueTask<ModelState<ItemModel>?> Transform(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref this.transformedCount);

        var model = new ItemModel
        {
            Id = envelope.ModelId,
            EventNumber = envelope.EventNumber,
            GlobalEventPosition = envelope.GlobalEventPosition,
            Name = "ok",
        };

        return ValueTask.FromResult<ModelState<ItemModel>?>(
            new ModelState<ItemModel>(
                model,
                IsNew: false,
                ShouldDelete: false,
                GlobalEventPosition: envelope.GlobalEventPosition,
                ExpectedEventNumber: null));
    }

    public void OnFlushSucceeded(
        IReadOnlyCollection<Guid> flushedModelIds,
        IReadOnlyCollection<Guid> clearModelIds,
        IReadOnlyDictionary<Guid, long?> flushedEventNumbers)
    {
        this.clearedModelIds.AddRange(clearModelIds);
    }
}
