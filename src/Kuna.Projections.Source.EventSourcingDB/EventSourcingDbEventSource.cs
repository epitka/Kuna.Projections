using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using EventSourcingDb;
using EventSourcingDb.Types;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Logging;
using EsdbEvent = EventSourcingDb.Types.Event;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// EventSourcingDB-backed <see cref="IEventSource{TEnvelope}"/> that observes the
/// configured subject, converts source events into projection envelopes, and yields
/// them to the pipeline with retry semantics.
/// </summary>
/// <remarks>
/// EventSourcingDB exposes events in a single, globally ordered stream and provides
/// no caught-up marker on the observe stream. The source therefore reads the head
/// position once on start and synthesizes a <see cref="ProjectionCaughtUpEvent"/>
/// either immediately (when the checkpoint is already at or beyond the head) or once
/// an observed event reaches the head. The signal is idempotent; missing or
/// imprecise signaling only affects live-processing latency, never correctness.
/// </remarks>
public class EventSourcingDbEventSource<TState> : IEventSource<EventEnvelope>
    where TState : class, IModel, new()
{
    private const string CaughtUpStreamId = "$projection-caught-up";
    private const int MaxConsecutiveObserveFailures = 10;

    private readonly IClient client;
    private readonly IEventEnvelopeFactory envelopeFactory;
    private readonly ICheckpointSerializer<string> checkpointSerializer;
    private readonly IHeadPositionReader headPositionReader;
    private readonly string subject;
    private readonly bool recursive;
    private readonly string instanceId;
    private readonly ILogger logger;

    /// <summary>
    /// Initializes the EventSourcingDB-backed event source for the specified
    /// projection model state type.
    /// </summary>
    public EventSourcingDbEventSource(
        IClient client,
        IEventEnvelopeFactory envelopeFactory,
        ICheckpointSerializer<string> checkpointSerializer,
        IHeadPositionReader headPositionReader,
        EventSourcingDbSourceSettings sourceSettings,
        IProjectionSettings<TState> projectionSettings,
        ILogger<EventSourcingDbEventSource<TState>> logger)
    {
        if (projectionSettings.EventVersionCheckStrategy == EventVersionCheckStrategy.Consecutive)
        {
            throw new InvalidOperationException(
                $"EventSourcingDB source for projection '{ProjectionModelName.For<TState>()}' is configured with "
                + "EventVersionCheckStrategy.Consecutive, which requires gapless per-aggregate version numbers. "
                + "EventSourcingDB exposes only a global, monotonic event id, so configure "
                + "EventVersionCheckStrategy.Monotonic (recommended) or EventVersionCheckStrategy.Disabled.");
        }

        this.client = client;
        this.envelopeFactory = envelopeFactory;
        this.checkpointSerializer = checkpointSerializer;
        this.headPositionReader = headPositionReader;
        this.subject = sourceSettings.Subject;
        this.recursive = sourceSettings.Recursive;
        this.instanceId = projectionSettings.InstanceId;
        this.logger = logger;
    }

    /// <summary>
    /// Reads projection envelopes from the configured EventSourcingDB subject starting
    /// at the provided global position.
    /// </summary>
    public IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        CancellationToken cancellationToken)
    {
        return this.ReadAllCore(start, cancellationToken);
    }

    private static bool HasReachedOrPassedHead(GlobalEventPosition start, ulong? headPosition)
    {
        if (headPosition is null)
        {
            return true;
        }

        if (!ulong.TryParse(start.Value, out var startId))
        {
            return false;
        }

        return startId >= headPosition.Value;
    }

    private static bool HasReachedHead(string eventId, ulong? headPosition)
    {
        if (headPosition is null)
        {
            return false;
        }

        return ulong.TryParse(eventId, out var id)
               && id >= headPosition.Value;
    }

    private static EventEnvelope CreateCaughtUpEnvelope(GlobalEventPosition position)
    {
        return new EventEnvelope(
            eventNumber: -1,
            streamPosition: position,
            streamId: CaughtUpStreamId,
            @event: new ProjectionCaughtUpEvent(),
            modelId: Guid.Empty,
            createdOn: DateTime.UtcNow);
    }

    private async IAsyncEnumerable<EventEnvelope> ReadAllCore(
        GlobalEventPosition start,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var retryBudget = new ObserveRetryBudget(MaxConsecutiveObserveFailures);
        var currentStart = start;
        var caughtUpEmitted = false;

        var headPosition = await this.TryReadHeadPositionAsync(cancellationToken);

        if (HasReachedOrPassedHead(currentStart, headPosition))
        {
            caughtUpEmitted = true;
            this.LogCaughtUp();
            yield return CreateCaughtUpEnvelope(currentStart);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            await using var enumerator = this.ObserveOnce(
                                                 currentStart,
                                                 headPosition,
                                                 caughtUpEmitted,
                                                 position => currentStart = position,
                                                 () => caughtUpEmitted = true,
                                                 cancellationToken)
                                             .GetAsyncEnumerator(cancellationToken);

            while (true)
            {
                EventEnvelope envelope;

                try
                {
                    if (!await enumerator.MoveNextAsync())
                    {
                        break;
                    }

                    envelope = enumerator.Current;

                    // A successfully observed event clears the consecutive-failure
                    // count, so the retry limit only ever fires on an uninterrupted
                    // run of failures, not on transient disconnects that each recover
                    // over the lifetime of a long-running projection.
                    retryBudget.RecordSuccess();
                }
                catch (Exception ex)
                {
                    if (cancellationToken.IsCancellationRequested
                        && ex is OperationCanceledException)
                    {
                        yield break;
                    }

                    var exhausted = retryBudget.RecordFailureAndCheckExhausted();
                    this.logger.LogWarning(ex, "Observe subscription dropped, consecutive failure {Attempt}", retryBudget.ConsecutiveFailures);

                    if (exhausted)
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

    private async IAsyncEnumerable<EventEnvelope> ObserveOnce(
        GlobalEventPosition start,
        ulong? headPosition,
        bool caughtUpAlreadyEmitted,
        Action<GlobalEventPosition> onProgress,
        Action onCaughtUp,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var caughtUp = caughtUpAlreadyEmitted;
        var lastObservedPosition = start;
        var lowerBoundId = this.checkpointSerializer.Deserialize(start);

        var options = new ObserveEventsOptions(
            Recursive: this.recursive,
            LowerBound: string.IsNullOrEmpty(lowerBoundId)
                            ? null
                            : new Bound(lowerBoundId, BoundType.Exclusive));

        await foreach (var sourceEvent in this.client.ObserveEventsAsync(this.subject, options, cancellationToken))
        {
            var position = new GlobalEventPosition(sourceEvent.Id);
            var envelope = this.TryCreateEnvelope(sourceEvent);

            lastObservedPosition = position;
            onProgress(position);

            if (envelope.HasValue)
            {
                yield return envelope.Value;
            }

            if (!caughtUp
                && HasReachedHead(sourceEvent.Id, headPosition))
            {
                caughtUp = true;
                onCaughtUp();
                this.LogCaughtUp();

                yield return CreateCaughtUpEnvelope(lastObservedPosition);
            }
        }
    }

    private EventEnvelope? TryCreateEnvelope(EsdbEvent sourceEvent)
    {
        var eventNumber = long.TryParse(sourceEvent.Id, out var parsed) ? parsed : 0L;

        return this.envelopeFactory.Create(
            subject: sourceEvent.Subject,
            data: sourceEvent.Data,
            eventType: sourceEvent.Type,
            eventNumber: eventNumber,
            eventPosition: new GlobalEventPosition(sourceEvent.Id),
            eventTime: sourceEvent.Time.UtcDateTime);
    }

    private async Task<ulong?> TryReadHeadPositionAsync(CancellationToken cancellationToken)
    {
        try
        {
            var headId = await this.headPositionReader.ReadHeadPositionAsync(cancellationToken);

            if (headId != null
                && ulong.TryParse(headId, out var parsed))
            {
                return parsed;
            }

            return null;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            this.logger.LogWarning(
                ex,
                "Could not read head position for {ModelName} instance {InstanceId}; treating projection as caught up",
                ProjectionModelName.For<TState>(),
                this.instanceId);

            return null;
        }
    }

    private void LogCaughtUp()
    {
        this.logger.LogInformation(
            "EventSourcingDB subscription caught up for {ModelName} instance {InstanceId}",
            ProjectionModelName.For<TState>(),
            this.instanceId);
    }
}
