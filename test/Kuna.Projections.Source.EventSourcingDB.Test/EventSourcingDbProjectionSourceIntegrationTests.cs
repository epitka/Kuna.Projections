using EventSourcingDb;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;
using EventCandidate = EventSourcingDb.Types.EventCandidate;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

[Collection(EventSourcingDbCollection.Name)]
public class EventSourcingDbProjectionSourceIntegrationTests
{
    private const string EventSource = "https://github.com/thenativeweb/kuna-projections-test";
    private const string SourceEventType = "io.kuna.test.SourceIntegrationEvent";
    private const string SubjectKeyedEventType = "io.kuna.test.SubjectKeyedEvent";

    private readonly EventSourcingDbContainerFixture fixture;

    public EventSourcingDbProjectionSourceIntegrationTests(EventSourcingDbContainerFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task ReadAll_Should_Return_Deserialized_Envelope_For_Event_In_Subject_Scope()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();
        var modelId = Guid.NewGuid();

        await this.WriteEventAsync(
            $"{root}/{modelId:N}",
            SourceEventType,
            new { aggregateId = modelId, name = "match", });

        var source = this.CreateSource(loggerFactory, root);
        var envelopes = await ReadUntilAsync(source, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));

        var real = envelopes.Where(x => x.Event is SourceIntegrationEvent).ToList();
        real.Count.ShouldBe(1);
        real[0].ModelId.ShouldBe(modelId);
        real[0].StreamId.ShouldBe($"{root}/{modelId:N}");
        ((SourceIntegrationEvent)real[0].Event).Name.ShouldBe("match");
        real[0].Event.CreatedOn.ShouldNotBe(default);
    }

    [Fact]
    public async Task ReadAll_Should_Emit_CaughtUp_After_Replaying_Existing_Events()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();

        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "first", });
        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "second", });

        var source = this.CreateSource(loggerFactory, root);
        var envelopes = await ReadUntilAsync(source, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));

        var caughtUpIndex = envelopes.FindIndex(x => x.Event is ProjectionCaughtUpEvent);
        caughtUpIndex.ShouldBeGreaterThanOrEqualTo(0);
        envelopes.Take(caughtUpIndex).Count(x => x.Event is SourceIntegrationEvent).ShouldBe(2);
    }

    [Fact]
    public async Task ReadAll_Should_Emit_CaughtUp_Immediately_When_Checkpoint_Is_At_Head()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();

        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "first", });
        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "second", });

        var drainSource = this.CreateSource(loggerFactory, root);
        var drained = await ReadUntilAsync(drainSource, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));
        var checkpoint = drained.Last(x => x.Event is SourceIntegrationEvent).GlobalEventPosition;

        var resumeSource = this.CreateSource(loggerFactory, root);
        var resumed = await ReadUntilAsync(resumeSource, checkpoint, list => list.Count >= 1, TimeSpan.FromSeconds(15));

        resumed[0].Event.ShouldBeOfType<ProjectionCaughtUpEvent>();
    }

    [Fact]
    public async Task ReadAll_Should_Resume_After_Checkpoint_Without_Replaying_Older_Events()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();

        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "first", });
        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "second", });

        var source = this.CreateSource(loggerFactory, root);
        var drained = await ReadUntilAsync(source, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));
        var checkpoint = drained.Last(x => x.Event is SourceIntegrationEvent).GlobalEventPosition;

        await this.WriteEventAsync($"{root}/{Guid.NewGuid():N}", SourceEventType, new { aggregateId = Guid.NewGuid(), name = "third", });

        var resumeSource = this.CreateSource(loggerFactory, root);
        var resumed = await ReadUntilAsync(resumeSource, checkpoint, HasCaughtUp, TimeSpan.FromSeconds(30));

        var real = resumed.Where(x => x.Event is SourceIntegrationEvent).ToList();
        real.Count.ShouldBe(1);
        ((SourceIntegrationEvent)real[0].Event).Name.ShouldBe("third");
        real[0].GlobalEventPosition.Value.ShouldNotBe(checkpoint.Value);
    }

    [Fact]
    public async Task ReadAll_Should_Drop_Event_When_ModelId_Cannot_Be_Resolved()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();

        await this.WriteEventAsync(
            $"{root}/unresolvable",
            SubjectKeyedEventType,
            new { name = "drop-me", });

        var source = this.CreateSource(
            loggerFactory,
            root,
            new[] { typeof(SourceIntegrationEvent), typeof(SubjectKeyedEvent), },
            ModelIdResolutionStrategy.UseStreamId);

        var envelopes = await ReadUntilAsync(source, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));

        envelopes.Count(x => x.Event is not ProjectionCaughtUpEvent).ShouldBe(0);
    }

    [Fact]
    public async Task ReadAll_Should_Resolve_ModelId_From_Subject_When_No_Attribute()
    {
        using var loggerFactory = LoggerFactory.Create(
            builder =>
            {
            });

        var root = NewRoot();
        var modelId = Guid.NewGuid();

        await this.WriteEventAsync(
            $"{root}/{modelId:N}",
            SubjectKeyedEventType,
            new { name = "subject-keyed", });

        var source = this.CreateSource(
            loggerFactory,
            root,
            new[] { typeof(SourceIntegrationEvent), typeof(SubjectKeyedEvent), },
            ModelIdResolutionStrategy.UseStreamId);

        var envelopes = await ReadUntilAsync(source, FromStart(), HasCaughtUp, TimeSpan.FromSeconds(30));

        var real = envelopes.Where(x => x.Event is SubjectKeyedEvent).ToList();
        real.Count.ShouldBe(1);
        real[0].ModelId.ShouldBe(modelId);
    }

    private static GlobalEventPosition FromStart()
    {
        return new GlobalEventPosition(string.Empty);
    }

    private static string NewRoot()
    {
        return $"/it{Guid.NewGuid():N}";
    }

    private static bool HasCaughtUp(IReadOnlyList<EventEnvelope> envelopes)
    {
        return envelopes.Any(x => x.Event is ProjectionCaughtUpEvent);
    }

    private static async Task<List<EventEnvelope>> ReadUntilAsync(
        EventSourcingDbEventSource<SourceIntegrationModel> source,
        GlobalEventPosition start,
        Func<IReadOnlyList<EventEnvelope>, bool> stop,
        TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var result = new List<EventEnvelope>();

        try
        {
            await foreach (var envelope in source.ReadAll(start, cts.Token))
            {
                result.Add(envelope);

                if (stop(result))
                {
                    await cts.CancelAsync();
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Expected once the stop condition cancels the open observe subscription.
        }

        return result;
    }

    private async Task WriteEventAsync(string subject, string type, object data)
    {
        await this.fixture.Client.WriteEventsAsync(
            new[] { new EventCandidate(EventSource, subject, type, data), },
            token: TestContext.Current.CancellationToken);
    }

    private EventSourcingDbEventSource<SourceIntegrationModel> CreateSource(
        ILoggerFactory loggerFactory,
        string subject,
        Type[]? eventTypes = null,
        ModelIdResolutionStrategy modelIdResolutionStrategy = ModelIdResolutionStrategy.UseModelIdAttribute)
    {
        var deserializer = new EventDeserializer(
            eventTypes ?? new[] { typeof(SourceIntegrationEvent), },
            null,
            loggerFactory.CreateLogger<EventDeserializer>());

        var resolver = new EventSourcingDbModelIdResolver(
            loggerFactory.CreateLogger<EventSourcingDbModelIdResolver>(),
            modelIdResolutionStrategy);

        var envelopeFactory = new EventEnvelopeFactory(deserializer, resolver);

        var headPositionReader = new EventSourcingDbHeadPositionReader(this.fixture.Client, subject, recursive: true);

        return new EventSourcingDbEventSource<SourceIntegrationModel>(
            this.fixture.Client,
            envelopeFactory,
            new EventSourcingDbCheckpointSerializer(),
            headPositionReader,
            new EventSourcingDbSourceSettings
            {
                Subject = subject,
                Recursive = true,
            },
            new ProjectionSettings<SourceIntegrationModel>
            {
                InstanceId = "source-integration",
                EventVersionCheckStrategy = EventVersionCheckStrategy.Monotonic,
            },
            loggerFactory.CreateLogger<EventSourcingDbEventSource<SourceIntegrationModel>>());
    }

    private sealed class SourceIntegrationModel : Model
    {
    }

    private sealed class SourceIntegrationEvent : Event
    {
        [ModelId]
        public Guid AggregateId { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class SubjectKeyedEvent : Event
    {
        public string Name { get; set; } = string.Empty;
    }
}
