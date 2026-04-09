using Kuna.Projections.Abstractions.Messages;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Applies source envelopes to model state and produces model-state changes for persistence.
/// </summary>
public interface IModelStateTransformer<in TEnvelope, TState>
    where TEnvelope : IEventEnvelope
{
    ValueTask<ModelState<TState>?> Transform(TEnvelope envelope, CancellationToken cancellationToken);
}
