using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kurrent.Extensions;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.Kurrent;

/// <summary>
/// Kurrent-backed <see cref="IEventSource{TEnvelope}"/> that subscribes to the
/// filtered event stream, converts source events into projection envelopes, and
/// yields them to the pipeline with retry and bounded buffering.
/// </summary>
public class KurrentDbEventSource<TState> : IEventSource<EventEnvelope>
    where TState : class, IModel, new()
{
    private readonly KurrentDBClient eventStoreClient;
    private readonly IEventEnvelopeFactory envelopeFactory;
    private readonly IProjectionSettings<TState> projectionSettings;
    private readonly SubscriptionFilterOptions filterOptions;
    private readonly ILogger logger;

    /// <summary>
    /// Initializes the Kurrent-backed event source for the specified projection
    /// model state type.
    /// </summary>
    public KurrentDbEventSource(
        KurrentDBClient eventStoreClient,
        IEventEnvelopeFactory envelopeFactory,
        KurrentDbSourceSettings sourceSettings,
        IProjectionSettings<TState> projectionSettings,
        ILogger<KurrentDbEventSource<TState>> logger)
    {
        this.eventStoreClient = eventStoreClient;
        this.envelopeFactory = envelopeFactory;
        this.projectionSettings = projectionSettings;
        this.filterOptions = KurrentDbSubscriptionFilterFactory.Create(sourceSettings.Filter);
        this.logger = logger;
    }

    /// <summary>
    /// Reads projection envelopes from the configured Kurrent stream starting at
    /// the provided global position.
    /// </summary>
    public async IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateBounded<EventEnvelope>(
            new BoundedChannelOptions(this.projectionSettings.ReadBufferCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            });

        _ = Task.Run(
            () => this.RunSubscriptionAsync(start, channel.Writer, cancellationToken),
            cancellationToken);

        await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return item;
        }
    }

    private async Task RunSubscriptionAsync(
        GlobalEventPosition start,
        ChannelWriter<EventEnvelope> writer,
        CancellationToken cancellationToken)
    {
        var attempts = 0;
        var currentStart = start;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await this.SubscribeOnce(
                    currentStart,
                    writer,
                    position => currentStart = position,
                    cancellationToken);

                writer.TryComplete();
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                writer.TryComplete();
                return;
            }
            catch (Exception ex)
            {
                attempts++;
                this.logger.LogWarning(ex, "Subscription dropped, attempt {Attempt}", attempts);

                if (attempts >= 10)
                {
                    writer.TryComplete(ex);
                    return;
                }

                await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            }
        }
    }

    private async Task SubscribeOnce(
        GlobalEventPosition start,
        ChannelWriter<EventEnvelope> writer,
        Action<GlobalEventPosition> onProgress,
        CancellationToken cancellationToken)
    {
        var lastObservedPosition = start;
        var position = new CheckPoint
        {
            ModelName = ProjectionModelName.For<TState>(),
            GlobalEventPosition = start,
        }.ToKurrentDbPosition();

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

                    if (envelope.HasValue)
                    {
                        await writer.WriteAsync(envelope.Value, cancellationToken);
                    }

                    // Advance retry checkpoint for all observed events, including dropped ones.
                    lastObservedPosition = eventPosition;
                    onProgress(eventPosition);
                    break;
                case StreamMessage.CaughtUp:
                    await writer.WriteAsync(
                        new EventEnvelope(
                            eventNumber: -1,
                            streamPosition: lastObservedPosition,
                            streamId: "$projection-caught-up",
                            @event: new ProjectionCaughtUpEvent(),
                            modelId: Guid.Empty,
                            createdOn: DateTime.UtcNow),
                        cancellationToken);

                    this.logger.LogInformation("KurrentDB subscription caught up for {ModelName}", ProjectionModelName.For<TState>());
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
