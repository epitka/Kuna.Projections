using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class FastSource : IEventSource<EventEnvelope>
{
    private readonly IReadOnlyList<EventEnvelope> envelopes;

    public FastSource(IReadOnlyList<EventEnvelope> envelopes)
    {
        this.envelopes = envelopes;
    }

    public async IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var envelope in this.envelopes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return envelope;
            await Task.Yield();
        }
    }
}
