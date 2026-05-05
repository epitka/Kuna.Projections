using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.KurrentDB.Extensions;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.KurrentDB;

/// <summary>
/// Kurrent-backed <see cref="IEventSource{TEnvelope}"/> that subscribes to the
/// filtered event stream, converts source events into projection envelopes, and
/// yields them to the pipeline with retry semantics.
/// </summary>
public class KurrentDbEventSource<TState> : IEventSource<EventEnvelope>
    where TState : class, IModel, new()
{
    private readonly KurrentDBClient eventStoreClient;
    private readonly IEventEnvelopeFactory envelopeFactory;
    private readonly ICheckpointSerializer<Position> checkpointSerializer;
    private readonly SubscriptionFilterOptions filterOptions;
    private readonly ILogger logger;

    /// <summary>
    /// Initializes the Kurrent-backed event source for the specified projection
    /// model state type.
    /// </summary>
    public KurrentDbEventSource(
        KurrentDBClient eventStoreClient,
        IEventEnvelopeFactory envelopeFactory,
        ICheckpointSerializer<Position> checkpointSerializer,
        KurrentDbSourceSettings sourceSettings,
        ILogger<KurrentDbEventSource<TState>> logger)
    {
        this.eventStoreClient = eventStoreClient;
        this.envelopeFactory = envelopeFactory;
        this.checkpointSerializer = checkpointSerializer;
        this.filterOptions = KurrentDbSubscriptionFilterFactory.Create(sourceSettings.Filter);
        this.logger = logger;
    }

    /// <summary>
    /// Reads projection envelopes from the configured Kurrent stream starting at
    /// the provided global position.
    /// </summary>
    public IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        CancellationToken cancellationToken)
    {
        return this.ReadAllCore(start, cancellationToken);
    }

    private async IAsyncEnumerable<EventEnvelope> ReadAllCore(
        GlobalEventPosition start,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var attempts = 0;
        var currentStart = start;

        while (!cancellationToken.IsCancellationRequested)
        {
            await using var enumerator = this.SubscribeOnce(
                                                 currentStart,
                                                 position => currentStart = position,
                                                 cancellationToken)
                                             .GetAsyncEnumerator(cancellationToken);

            while (true)
            {
                EventEnvelope envelope;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                    {
                        yield break;
                    }

                    envelope = enumerator.Current;
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested
                        && ex is OperationCanceledException)
                    {
                        yield break;
                    }

                    attempts++;
                    this.logger.LogWarning(ex, "Subscription dropped, attempt {Attempt}", attempts);

                    if (attempts >= 10)
                    {
                        ExceptionDispatchInfo.Capture(ex).Throw();
                    }

                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        yield break;
                    }

                    break;
                }

                yield return envelope;
            }
        }
    }

    private async IAsyncEnumerable<EventEnvelope> SubscribeOnce(
        GlobalEventPosition start,
        Action<GlobalEventPosition> onProgress,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lastObservedPosition = start;
        var position = this.checkpointSerializer.Deserialize(start);

        await using var subscription = this.eventStoreClient.SubscribeToAll(
            FromAll.After(position),
            filterOptions: this.filterOptions,
            cancellationToken: cancellationToken);

        await foreach (var message in subscription.Messages.WithCancellation(cancellationToken))
        {
            switch (message)
            {
                case StreamMessage.Event(var evnt):
                    var eventPosition = evnt.OriginalPosition.HasValue
                                            ? evnt.OriginalPosition.Value.ToGlobalEventPosition()
                                            : Position.Start.ToGlobalEventPosition();

                    var envelope = this.TryCreateEnvelope(evnt);

                    // Advance retry checkpoint for all observed events, including dropped ones.
                    lastObservedPosition = eventPosition;
                    onProgress(eventPosition);

                    if (envelope.HasValue)
                    {
                        yield return envelope.Value;
                    }

                    break;
                case StreamMessage.CaughtUp:
                    this.logger.LogInformation("KurrentDB subscription caught up for {ModelName}", ProjectionModelName.For<TState>());

                    yield return new EventEnvelope(
                        eventNumber: -1,
                        streamPosition: lastObservedPosition,
                        streamId: "$projection-caught-up",
                        @event: new ProjectionCaughtUpEvent(),
                        modelId: Guid.Empty,
                        createdOn: DateTime.UtcNow);

                    break;
            }
        }
    }

    private EventEnvelope? TryCreateEnvelope(ResolvedEvent resolvedEvent)
    {
        var msg = this.envelopeFactory.Create(
            streamId: resolvedEvent.Event.EventStreamId,
            eventData: resolvedEvent.Event.Data.ToArray(),
            eventType: resolvedEvent.Event.EventType,
            eventNumber: resolvedEvent.Event.EventNumber.ToInt64(),
            eventPosition: resolvedEvent.OriginalPosition.HasValue
                               ? resolvedEvent.OriginalPosition.Value.ToGlobalEventPosition()
                               : Position.Start.ToGlobalEventPosition(),
            eventTime: resolvedEvent.Event.Created);

        return msg;
    }
}
