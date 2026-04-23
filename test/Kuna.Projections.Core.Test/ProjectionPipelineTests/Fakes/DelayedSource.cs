using System.Runtime.CompilerServices;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class DelayedSource : IEventSource<EventEnvelope>
{
    private readonly IReadOnlyList<EventEnvelope> envelopes;
    private readonly TimeSpan delay;

    public DelayedSource(IReadOnlyList<EventEnvelope> envelopes, TimeSpan delay)
    {
        this.envelopes = envelopes;
        this.delay = delay;
    }

    public async IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var envelope in this.envelopes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return envelope;
            await Task.Delay(this.delay, cancellationToken);
        }
    }
}
