using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Reads projection envelopes from a source starting at a given global position.
/// </summary>
public interface IEventSource<out TEnvelope>
    where TEnvelope : IEventEnvelope
{
    IAsyncEnumerable<TEnvelope> ReadAll(GlobalEventPosition start, CancellationToken cancellationToken);
}
