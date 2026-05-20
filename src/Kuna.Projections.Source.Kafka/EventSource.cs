using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.Kafka;

public sealed class EventSource<TState> : IEventSource<EventEnvelope>
    where TState : class, IModel, new()
{
    private readonly IConsumerFactory consumerFactory;
    private readonly ISourceTransformer transformer;
    private readonly EventEnvelopeFactory envelopeFactory;
    private readonly ICheckpointSerializer<Checkpoint> checkpointSerializer;
    private readonly KafkaSourceSettings sourceSettings;
    private readonly IProjectionSettings<TState> projectionSettings;
    private readonly ILogger<EventSource<TState>> logger;

    public EventSource(
        IConsumerFactory consumerFactory,
        ISourceTransformer transformer,
        EventEnvelopeFactory envelopeFactory,
        ICheckpointSerializer<Checkpoint> checkpointSerializer,
        KafkaSourceSettings sourceSettings,
        IProjectionSettings<TState> projectionSettings,
        ILogger<EventSource<TState>> logger)
    {
        this.consumerFactory = consumerFactory;
        this.transformer = transformer;
        this.envelopeFactory = envelopeFactory;
        this.checkpointSerializer = checkpointSerializer;
        this.sourceSettings = sourceSettings;
        this.projectionSettings = projectionSettings;
        this.logger = logger;
    }

    public async IAsyncEnumerable<EventEnvelope> ReadAll(
        GlobalEventPosition start,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var checkpoint = this.checkpointSerializer.Deserialize(start);
        ValidateCheckpointTopic(checkpoint, this.sourceSettings.Topic);

        var consumerGroupId = ConsumerGroupIdResolver.ResolveProjection(this.sourceSettings, this.projectionSettings);
        using var consumer = this.consumerFactory.Create(this.sourceSettings, consumerGroupId);
        var assignedPartitions = ResolveAssignedPartitions(consumer);
        var checkpointOffsets = checkpoint.Partitions
                                          .Where(x => assignedPartitions.Contains(x.Key))
                                          .ToDictionary(x => x.Key, x => x.Value);

        var currentOffsets = InitializeOffsets(checkpointOffsets, assignedPartitions);
        var caughtUpEmitted = false;

        consumer.Assign(this.sourceSettings.Topic, assignedPartitions, checkpointOffsets);

        this.logger.LogInformation(
            "Kafka source starting for {ModelName} instance {InstanceId}: topic={Topic}, partitions=[{Partitions}], startCheckpoint={StartCheckpoint}",
            ProjectionModelName.For<TState>(),
            this.projectionSettings.InstanceId,
            this.sourceSettings.Topic,
            string.Join(", ", assignedPartitions),
            start);

        while (!cancellationToken.IsCancellationRequested)
        {
            var message = consumer.Consume(TimeSpan.FromMilliseconds(this.sourceSettings.PollTimeoutMs), cancellationToken);

            if (message is null)
            {
                if (!caughtUpEmitted
                    && IsCaughtUp(consumer, assignedPartitions, currentOffsets))
                {
                    caughtUpEmitted = true;

                    yield return new EventEnvelope(
                        eventNumber: -1,
                        streamPosition: SerializeOffsets(currentOffsets),
                        streamId: "$projection-caught-up",
                        @event: new ProjectionCaughtUpEvent(),
                        modelId: Guid.Empty,
                        createdOn: DateTime.UtcNow);
                }

                await Task.Yield();
                continue;
            }

            if (caughtUpEmitted)
            {
                caughtUpEmitted = false;

                this.logger.LogInformation(
                    "Kafka source received new records after catch-up for {ModelName} instance {InstanceId}: topic={Topic}, partition={Partition}, offset={Offset}",
                    ProjectionModelName.For<TState>(),
                    this.projectionSettings.InstanceId,
                    message.Topic,
                    message.Partition,
                    message.Offset);
            }

            currentOffsets[message.Partition] = message.Offset;

            var sourceRecord = this.transformer.Transform(
                new SourceRecordContext
                {
                    Topic = message.Topic,
                    Partition = message.Partition,
                    Offset = message.Offset,
                    KeyBytes = message.KeyBytes,
                    ValueBytes = message.ValueBytes,
                    Headers = message.Headers,
                    TimestampUtc = message.TimestampUtc,
                });

            yield return this.envelopeFactory.Create(sourceRecord, SerializeOffsets(currentOffsets));
            await Task.Yield();
        }
    }

    private static void ValidateCheckpointTopic(
        Checkpoint checkpoint,
        string configuredTopic)
    {
        if (string.IsNullOrWhiteSpace(checkpoint.Topic))
        {
            return;
        }

        if (!string.Equals(checkpoint.Topic, configuredTopic, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Checkpoint topic '{checkpoint.Topic}' does not match configured topic '{configuredTopic}'.");
        }
    }

    private static Dictionary<int, long> InitializeOffsets(
        IReadOnlyDictionary<int, long> checkpointOffsets,
        IReadOnlyList<int> assignedPartitions)
    {
        var currentOffsets = checkpointOffsets.ToDictionary(x => x.Key, x => x.Value);

        foreach (var partition in assignedPartitions)
        {
            _ = currentOffsets.TryAdd(partition, -1);
        }

        return currentOffsets;
    }

    private IReadOnlyList<int> ResolveAssignedPartitions(IConsumer consumer)
    {
        var discoveredPartitions = consumer.GetPartitions(this.sourceSettings.Topic);

        if (discoveredPartitions.Count == 0)
        {
            throw new InvalidOperationException($"Kafka topic '{this.sourceSettings.Topic}' has no partitions.");
        }

        if (this.sourceSettings.Partitions is { Length: > 0, })
        {
            var missingPartitions = this.sourceSettings.Partitions
                                        .Except(discoveredPartitions)
                                        .OrderBy(x => x)
                                        .ToArray();

            if (missingPartitions.Length > 0)
            {
                throw new InvalidOperationException(
                    $"Kafka topic '{this.sourceSettings.Topic}' does not contain configured partitions: {string.Join(", ", missingPartitions)}.");
            }

            return this.sourceSettings.Partitions.OrderBy(x => x).ToArray();
        }

        return discoveredPartitions;
    }

    private GlobalEventPosition SerializeOffsets(IReadOnlyDictionary<int, long> currentOffsets)
    {
        return this.checkpointSerializer.Serialize(
            new Checkpoint
            {
                Topic = this.sourceSettings.Topic,
                Partitions = currentOffsets.ToDictionary(x => x.Key, x => x.Value),
            });
    }

    private bool IsCaughtUp(
        IConsumer consumer,
        IReadOnlyList<int> assignedPartitions,
        IReadOnlyDictionary<int, long> currentOffsets)
    {
        foreach (var partition in assignedPartitions)
        {
            var highWatermarkOffset = consumer.GetHighWatermarkOffset(this.sourceSettings.Topic, partition);
            var lastObservedOffset = currentOffsets.GetValueOrDefault(partition, -1);

            if (highWatermarkOffset == 0)
            {
                continue;
            }

            if (lastObservedOffset < highWatermarkOffset - 1)
            {
                return false;
            }
        }

        this.logger.LogInformation(
            "Kafka source caught up for {ModelName} instance {InstanceId}: topic={Topic}",
            ProjectionModelName.For<TState>(),
            this.projectionSettings.InstanceId,
            this.sourceSettings.Topic);

        return true;
    }
}
