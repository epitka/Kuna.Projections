using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaEventSource<TState> : IEventSource<EventEnvelope>
    where TState : class, IModel, new()
{
    public IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
