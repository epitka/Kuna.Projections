using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kurrent;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

[Collection(KurrentDbCollection.Name)]
public class KurrentDbProjectionSourceIntegrationTests
{
    private readonly KurrentDBContainerFixture fixture;

    public KurrentDbProjectionSourceIntegrationTests(KurrentDBContainerFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task ReadAll_Should_Return_Deserialized_Envelope_And_Filter_By_Stream_Prefix()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStream = $"{streamPrefix}{Guid.NewGuid():N}";
        var nonMatchingStream = $"other-it-{Guid.NewGuid():N}";
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.UtcNow;

        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            matchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = modelId,
                Name = "match",
                CreatedOn = createdOn,
                TypeName = nameof(SourceIntegrationEvent),
            });

        await AppendEvent(
            client,
            nonMatchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "ignore",
                CreatedOn = createdOn,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var source = CreateSource(loggerFactory, client, streamPrefix);
        var envelopes = await ReadEnvelopesUntilCount(source, expectedCount: 1, timeout: TimeSpan.FromSeconds(15));

        envelopes.Count.ShouldBe(1);
        var envelope = envelopes[0];
        envelope.StreamId.ShouldBe(matchingStream);
        envelope.ModelId.ShouldBe(modelId);
        envelope.EventNumber.ShouldBe(0);
        envelope.Event.ShouldBeOfType<SourceIntegrationEvent>();

        var @event = (SourceIntegrationEvent)envelope.Event;
        @event.AggregateId.ShouldBe(modelId);
        @event.Name.ShouldBe("match");
        @event.TypeName.ShouldBe(nameof(SourceIntegrationEvent));
    }

    [Fact]
    public async Task ReadAll_Should_Drop_Event_When_ModelId_Cannot_Be_Resolved()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStreamWithoutGuid = $"{streamPrefix}no-guid";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            matchingStreamWithoutGuid,
            nameof(NoModelIdSourceIntegrationEvent),
            new NoModelIdSourceIntegrationEvent
            {
                Name = "unresolvable",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(NoModelIdSourceIntegrationEvent),
            });

        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            eventTypes: new[] { typeof(SourceIntegrationEvent), typeof(NoModelIdSourceIntegrationEvent), });

        var envelopes = await ReadEnvelopesUntilCount(source, expectedCount: 1, timeout: TimeSpan.FromSeconds(2));

        envelopes.Count.ShouldBe(0);
    }

    [Fact]
    public async Task ReadAll_Should_Resume_After_Checkpoint_Position_And_Not_Replay_Older_Events()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            $"{streamPrefix}{Guid.NewGuid():N}",
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "first",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        await AppendEvent(
            client,
            $"{streamPrefix}{Guid.NewGuid():N}",
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "second",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var source = CreateSource(loggerFactory, client, streamPrefix);
        var initial = await ReadEnvelopesUntilCount(source, expectedCount: 2, timeout: TimeSpan.FromSeconds(15));

        initial.Count.ShouldBe(2);
        var checkpoint = initial.Max(x => x.GlobalEventPosition.Value);

        await AppendEvent(
            client,
            $"{streamPrefix}{Guid.NewGuid():N}",
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "third",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var resumed = await ReadEnvelopesUntilCount(
                          source,
                          expectedCount: 1,
                          timeout: TimeSpan.FromSeconds(15),
                          start: new GlobalEventPosition(checkpoint));

        resumed.Count.ShouldBe(1);
        resumed[0].Event.ShouldBeOfType<SourceIntegrationEvent>();
        ((SourceIntegrationEvent)resumed[0].Event).Name.ShouldBe("third");
        resumed[0].GlobalEventPosition.Value.ShouldBeGreaterThan(checkpoint);
    }

    [Fact]
    public async Task ReadAll_Should_Retry_When_EnvelopeFactory_Throws_Transiently_And_Event_Should_Be_Emitted()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStream = $"{streamPrefix}{Guid.NewGuid():N}";
        var modelId = Guid.NewGuid();

        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            matchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = modelId,
                Name = "retry-me",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var callCount = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new ThrowOnceEnvelopeFactory(inner, () => Interlocked.Increment(ref callCount)));

        var envelopes = await ReadEnvelopesUntilCount(source, expectedCount: 1, timeout: TimeSpan.FromSeconds(20));

        envelopes.Count.ShouldBe(1);
        envelopes[0].StreamId.ShouldBe(matchingStream);
        envelopes[0].ModelId.ShouldBe(modelId);
        ((SourceIntegrationEvent)envelopes[0].Event).Name.ShouldBe("retry-me");
        callCount.ShouldBeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task ReadAll_Should_Not_Create_Additional_Envelopes_Until_Consumer_Requests_Them()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";

        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        for (var i = 0; i < 3; i++)
        {
            await AppendEvent(
                client,
                $"{streamPrefix}{Guid.NewGuid():N}",
                "SourceIntegrationEvent",
                new SourceIntegrationEvent
                {
                    AggregateId = Guid.NewGuid(),
                    Name = $"evt-{i}",
                    CreatedOn = DateTime.UtcNow,
                    TypeName = nameof(SourceIntegrationEvent),
                });
        }

        var createCalls = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new CountingEnvelopeFactory(inner, () => Interlocked.Increment(ref createCalls)));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        await using var enumerator = source.ReadAll(new GlobalEventPosition(0), cts.Token).GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        createCalls.ShouldBe(1);

        await Task.Delay(TimeSpan.FromMilliseconds(500), cts.Token);
        createCalls.ShouldBe(1);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        createCalls.ShouldBe(2);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        createCalls.ShouldBe(3);
    }

    [Fact]
    public async Task ReadAll_Should_Fail_After_Retry_Limit_When_EnvelopeFactory_Always_Throws()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStream = $"{streamPrefix}{Guid.NewGuid():N}";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            matchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "boom",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var attempts = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new AlwaysThrowEnvelopeFactory(() => Interlocked.Increment(ref attempts)));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var ex = await Should.ThrowAsync<InvalidOperationException>(
                     async () =>
                     {
                         await foreach (var _ in source.ReadAll(new GlobalEventPosition(0), cts.Token))
                         {
                             // no-op
                         }
                     });

        ex.Message.ShouldContain("persistent test failure");
        attempts.ShouldBeGreaterThanOrEqualTo(10);
    }

    [Fact]
    public async Task ReadAll_Should_Stop_Cleanly_When_Cancelled_During_Retry_Delay()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStream = $"{streamPrefix}{Guid.NewGuid():N}";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            matchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "cancel-retry",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var attempts = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new AlwaysThrowEnvelopeFactory(() => Interlocked.Increment(ref attempts)));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var readTask = Task.Run(
            async () =>
            {
                try
                {
                    await foreach (var _ in source.ReadAll(new GlobalEventPosition(0), cts.Token))
                    {
                        // No envelopes expected.
                    }
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    // Expected on explicit cancellation.
                }
            },
            cts.Token);

        await WaitUntil(() => Volatile.Read(ref attempts) >= 1, TimeSpan.FromSeconds(10), cts.Token);
        cts.Cancel();

        await readTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        attempts.ShouldBeGreaterThanOrEqualTo(1);
        attempts.ShouldBeLessThan(10);
    }

    [Fact]
    public async Task ReadAll_Should_Not_Replay_Already_Emitted_Events_After_Retry()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            $"{streamPrefix}{Guid.NewGuid():N}",
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "first",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        await AppendEvent(
            client,
            $"{streamPrefix}{Guid.NewGuid():N}",
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "second",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var createCalls = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new ThrowOnSecondCreateEnvelopeFactory(
                inner,
                () => Interlocked.Increment(ref createCalls)));

        var envelopes = await ReadEnvelopesUntilCount(source, expectedCount: 2, timeout: TimeSpan.FromSeconds(20));
        var names = envelopes.OfType<EventEnvelope>()
                             .Select(x => x.Event)
                             .OfType<SourceIntegrationEvent>()
                             .Select(x => x.Name)
                             .ToList();

        names.Count.ShouldBe(2);
        names.ShouldContain("first");
        names.ShouldContain("second");
        names.Count(x => x == "first").ShouldBe(1);
        createCalls.ShouldBeGreaterThanOrEqualTo(3);
    }

    [Fact]
    public async Task ReadAll_Should_Resume_After_Dropped_Event_When_Retry_Happens_On_Later_Event()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            $"{streamPrefix}no-guid",
            nameof(NoModelIdSourceIntegrationEvent),
            new NoModelIdSourceIntegrationEvent
            {
                Name = "drop-me",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(NoModelIdSourceIntegrationEvent),
            });

        var modelId = Guid.NewGuid();
        await AppendEvent(
            client,
            $"{streamPrefix}{modelId:N}",
            nameof(SourceIntegrationEvent),
            new SourceIntegrationEvent
            {
                AggregateId = modelId,
                Name = "after-drop",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        var createCalls = 0;
        var source = CreateSource(
            loggerFactory,
            client,
            streamPrefix,
            envelopeFactoryDecorator: inner => new ThrowOnSecondCreateEnvelopeFactory(
                inner,
                () => Interlocked.Increment(ref createCalls)),
            eventTypes: new[] { typeof(SourceIntegrationEvent), typeof(NoModelIdSourceIntegrationEvent), });

        var envelopes = await ReadEnvelopesUntilCount(source, expectedCount: 1, timeout: TimeSpan.FromSeconds(20));

        envelopes.Count.ShouldBe(1);
        envelopes[0].ModelId.ShouldBe(modelId);
        envelopes[0].Event.ShouldBeOfType<SourceIntegrationEvent>();
        ((SourceIntegrationEvent)envelopes[0].Event).Name.ShouldBe("after-drop");
        createCalls.ShouldBe(3);
    }

    [Fact(Skip = "Restart stress test; excluded from the default test suite.")]
    public async Task ReadAll_Should_Continue_After_Kurrent_Container_Restart_Without_Replaying_Processed_Events()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var matchingStream = $"{streamPrefix}{Guid.NewGuid():N}";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var sourceClient = CreateClient();
        var source = CreateSource(loggerFactory, sourceClient, streamPrefix);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        var envelopes = new List<EventEnvelope>();
        var sync = new object();

        var readerTask = Task.Run(
            async () =>
            {
                try
                {
                    await foreach (var envelope in source.ReadAll(new GlobalEventPosition(0), cts.Token))
                    {
                        lock (sync)
                        {
                            envelopes.Add(envelope);
                        }
                    }
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    // Expected on test shutdown.
                }
            },
            cts.Token);

        await AppendEvent(
            CreateClient(),
            matchingStream,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "before-restart",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        await WaitUntil(
            () =>
            {
                lock (sync)
                {
                    return envelopes.Any(x => x.Event is SourceIntegrationEvent se && se.Name == "before-restart");
                }
            },
            TimeSpan.FromSeconds(20),
            cts.Token);

        await this.fixture.KurrentDBTestContainer.StopAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
        await this.fixture.KurrentDBTestContainer.StartAsync(cts.Token);

        await AppendEventWithRetries(
            matchingStream,
            "after-restart",
            retries: 20,
            delay: TimeSpan.FromSeconds(1),
            cts.Token);

        await WaitUntil(
            () =>
            {
                lock (sync)
                {
                    return envelopes.Any(x => x.Event is SourceIntegrationEvent se && se.Name == "after-restart");
                }
            },
            TimeSpan.FromSeconds(30),
            cts.Token);

        cts.Cancel();
        await readerTask;

        List<string> names;

        lock (sync)
        {
            names = envelopes
                    .OfType<EventEnvelope>()
                    .Select(x => x.Event)
                    .OfType<SourceIntegrationEvent>()
                    .Select(x => x.Name)
                    .ToList();
        }

        names.ShouldContain("before-restart");
        names.ShouldContain("after-restart");
        names.Count(x => x == "before-restart").ShouldBe(1);
    }

    [Fact(Skip = "Restart stress test; excluded from the default test suite.")]
    public async Task ReadAll_Should_Read_New_Events_After_Kurrent_Container_Restart_With_New_Source()
    {
        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        var streamName = $"{streamPrefix}{Guid.NewGuid():N}";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var client = CreateClient();

        await AppendEvent(
            client,
            streamName,
            "SourceIntegrationEvent",
            new SourceIntegrationEvent
            {
                AggregateId = Guid.NewGuid(),
                Name = "before-restart",
                CreatedOn = DateTime.UtcNow,
                TypeName = nameof(SourceIntegrationEvent),
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(90));
        await this.fixture.KurrentDBTestContainer.StopAsync(cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
        await this.fixture.KurrentDBTestContainer.StartAsync(cts.Token);

        await AppendEventWithRetries(
            streamName,
            "after-restart",
            retries: 20,
            delay: TimeSpan.FromSeconds(1),
            cts.Token);

        var resumedSource = CreateSource(loggerFactory, CreateClient(), streamPrefix);
        var resumed = await ReadEnvelopesUntilCount(
                          resumedSource,
                          expectedCount: 1,
                          timeout: TimeSpan.FromSeconds(30),
                          start: new GlobalEventPosition(0));

        resumed.Count.ShouldBe(1);
        resumed[0].Event.ShouldBeOfType<SourceIntegrationEvent>();
        ((SourceIntegrationEvent)resumed[0].Event).Name.ShouldBe("after-restart");
    }

    [Fact(Skip = "Auto-reconnect stress test; excluded from the default test suite.")]
    public async Task ReadAll_Should_Resume_Within_3s_After_MidStream_Connection_Drop_Without_Duplicates()
    {
        const int preDropEvents = 200;
        const int dropTriggerConsumed = 80;
        const int postReconnectEvents = 50;
        const int expectedTotal = preDropEvents + postReconnectEvents;

        var streamPrefix = $"orders-it-{Guid.NewGuid():N}-";
        using var loggerFactory = LoggerFactory.Create(
            _ =>
            {
            });

        var source = CreateSource(loggerFactory, CreateClient(), streamPrefix);

        var expectedNames = Enumerable
                            .Range(0, expectedTotal)
                            .Select(i => $"evt-{i:D4}")
                            .ToHashSet(StringComparer.Ordinal);

        using var seedClient = CreateClient();

        for (var i = 0; i < preDropEvents; i++)
        {
            await AppendEvent(
                seedClient,
                $"{streamPrefix}{Guid.NewGuid():N}",
                "SourceIntegrationEvent",
                new SourceIntegrationEvent
                {
                    AggregateId = Guid.NewGuid(),
                    Name = $"evt-{i:D4}",
                    CreatedOn = DateTime.UtcNow,
                    TypeName = nameof(SourceIntegrationEvent),
                });
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        var sync = new object();
        var names = new List<string>(expectedTotal);
        var positions = new List<ulong>(expectedTotal);
        var readerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var readerTask = Task.Run(
            async () =>
            {
                try
                {
                    await foreach (var envelope in source.ReadAll(new GlobalEventPosition(0), cts.Token))
                    {
                        readerStarted.TrySetResult();

                        if (envelope.Event is not SourceIntegrationEvent sourceEvent)
                        {
                            continue;
                        }

                        lock (sync)
                        {
                            names.Add(sourceEvent.Name);
                            positions.Add(envelope.GlobalEventPosition.Value);
                        }
                    }
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                    // Expected on test shutdown.
                }
            },
            cts.Token);

        await readerStarted.Task.WaitAsync(cts.Token);
        await WaitUntil(
            () =>
            {
                lock (sync)
                {
                    return names.Count >= dropTriggerConsumed;
                }
            },
            TimeSpan.FromSeconds(20),
            cts.Token);

        int consumedBeforeDrop;

        lock (sync)
        {
            consumedBeforeDrop = names.Count;
        }

        var containerName = GetKurrentContainerName();
        await RunDockerCommandAsync($"pause {containerName}", cts.Token);
        await Task.Delay(TimeSpan.FromSeconds(2), cts.Token);
        await RunDockerCommandAsync($"unpause {containerName}", cts.Token);
        var resumedStopwatch = Stopwatch.StartNew();

        for (var i = preDropEvents; i < expectedTotal; i++)
        {
            await AppendEventWithRetries(
                $"{streamPrefix}{Guid.NewGuid():N}",
                $"evt-{i:D4}",
                retries: 20,
                delay: TimeSpan.FromMilliseconds(250),
                cts.Token);
        }

        await WaitUntil(
            () =>
            {
                lock (sync)
                {
                    return names.Count > consumedBeforeDrop;
                }
            },
            TimeSpan.FromSeconds(20),
            cts.Token);

        resumedStopwatch.Stop();

        resumedStopwatch.Elapsed.ShouldBeLessThanOrEqualTo(TimeSpan.FromSeconds(3));

        await WaitUntil(
            () =>
            {
                lock (sync)
                {
                    return names.Count >= expectedTotal;
                }
            },
            TimeSpan.FromSeconds(45),
            cts.Token);

        cts.Cancel();
        await readerTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        List<string> finalNames;
        List<ulong> finalPositions;

        lock (sync)
        {
            finalNames = names.ToList();
            finalPositions = positions.ToList();
        }

        finalNames.Count.ShouldBe(expectedTotal);
        finalNames.Distinct(StringComparer.Ordinal).Count().ShouldBe(expectedTotal);

        foreach (var expectedName in expectedNames)
        {
            finalNames.ShouldContain(expectedName);
        }

        for (var i = 1; i < finalPositions.Count; i++)
        {
            finalPositions[i].ShouldBeGreaterThan(finalPositions[i - 1]);
        }
    }

    private static async Task AppendEvent<T>(
        KurrentDBClient client,
        string streamName,
        string eventType,
        T payload)
    {
        var eventData = new EventData(
            Uuid.NewUuid(),
            eventType,
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(payload)));

        await client.AppendToStreamAsync(
            streamName,
            StreamState.NoStream,
            new[] { eventData, },
            cancellationToken: CancellationToken.None);
    }

    private static async Task<List<EventEnvelope>> ReadEnvelopesUntilCount(
        KurrentDbEventSource<SourceIntegrationModel> source,
        int expectedCount,
        TimeSpan timeout,
        GlobalEventPosition? start = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        var result = new List<EventEnvelope>();
        var startPosition = start ?? new GlobalEventPosition(0);

        try
        {
            await foreach (var envelope in source.ReadAll(startPosition, cts.Token))
            {
                if (envelope.Event is ProjectionCaughtUpEvent)
                {
                    continue;
                }

                result.Add(envelope);

                if (result.Count >= expectedCount)
                {
                    cts.Cancel();
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Expected once we cancel after collecting enough events.
        }

        return result;
    }

    private static async Task WaitUntil(
        Func<bool> condition,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var started = DateTime.UtcNow;

        while (!condition())
        {
            if (DateTime.UtcNow - started > timeout)
            {
                throw new TimeoutException("Condition was not met within timeout.");
            }

            await Task.Delay(100, cancellationToken);
        }
    }

    private static async Task RunDockerCommandAsync(string arguments, CancellationToken cancellationToken)
    {
        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };

        process.Start();
        await process.WaitForExitAsync(cancellationToken);

        if (process.ExitCode == 0)
        {
            return;
        }

        var stderr = await process.StandardError.ReadToEndAsync(cancellationToken);
        throw new InvalidOperationException($"docker {arguments} failed with exit code {process.ExitCode}: {stderr}");
    }

    private static string GetKurrentContainerName()
    {
        var suffix = Environment.GetEnvironmentVariable("KUNA_TEST_CONTAINER_SUFFIX") ?? "default";
        return $"kuna-kurrent-source-it-{suffix}";
    }

    private KurrentDbEventSource<SourceIntegrationModel> CreateSource(
        ILoggerFactory loggerFactory,
        KurrentDBClient client,
        string streamPrefix,
        Func<IEventEnvelopeFactory, IEventEnvelopeFactory>? envelopeFactoryDecorator = null,
        Type[]? eventTypes = null)
    {
        var deserializer = new EventDeserializer(
            eventTypes ?? new[] { typeof(SourceIntegrationEvent), },
            loggerFactory.CreateLogger<EventDeserializer>());

        var resolver = new EventModelIdResolver(loggerFactory.CreateLogger<EventModelIdResolver>());
        IEventEnvelopeFactory envelopeFactory = new EventEnvelopeFactory(deserializer, resolver);
        envelopeFactory = envelopeFactoryDecorator?.Invoke(envelopeFactory) ?? envelopeFactory;

        return new KurrentDbEventSource<SourceIntegrationModel>(
            client,
            envelopeFactory,
            new KurrentDbSourceSettings
            {
                Filter = new KurrentDbFilterSettings
                {
                    Kind = KurrentDbFilterKind.StreamPrefix,
                    Prefixes = [streamPrefix,],
                },
            },
            loggerFactory.CreateLogger<KurrentDbEventSource<SourceIntegrationModel>>());
    }

    private async Task AppendEventWithRetries(
        string streamName,
        string name,
        int retries,
        TimeSpan delay,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < retries; i++)
        {
            try
            {
                await AppendEvent(
                    CreateClient(),
                    streamName,
                    "SourceIntegrationEvent",
                    new SourceIntegrationEvent
                    {
                        AggregateId = Guid.NewGuid(),
                        Name = name,
                        CreatedOn = DateTime.UtcNow,
                        TypeName = nameof(SourceIntegrationEvent),
                    });

                return;
            }
            catch when (i < retries - 1)
            {
                await Task.Delay(delay, cancellationToken);
            }
        }

        throw new InvalidOperationException("Failed to append after Kurrent restart");
    }

    private KurrentDBClient CreateClient()
    {
        var settings = KurrentDBClientSettings.Create(this.fixture.ConnectionString);
        return new KurrentDBClient(settings);
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

    private sealed class NoModelIdSourceIntegrationEvent : Event
    {
        public string Name { get; set; } = string.Empty;
    }

    private sealed class ThrowOnceEnvelopeFactory : IEventEnvelopeFactory
    {
        private readonly IEventEnvelopeFactory inner;
        private readonly Action onCreate;
        private int attempts;

        public ThrowOnceEnvelopeFactory(IEventEnvelopeFactory inner, Action onCreate)
        {
            this.inner = inner;
            this.onCreate = onCreate;
        }

        public EventEnvelope? Create(
            string streamId,
            byte[] eventData,
            string eventType,
            long eventNumber,
            GlobalEventPosition eventPosition,
            DateTime eventTime)
        {
            this.onCreate();

            if (Interlocked.Increment(ref this.attempts) == 1)
            {
                throw new InvalidOperationException("transient test failure");
            }

            return this.inner.Create(streamId, eventData, eventType, eventNumber, eventPosition, eventTime);
        }
    }

    private sealed class CountingEnvelopeFactory : IEventEnvelopeFactory
    {
        private readonly IEventEnvelopeFactory inner;
        private readonly Action onCreate;

        public CountingEnvelopeFactory(IEventEnvelopeFactory inner, Action onCreate)
        {
            this.inner = inner;
            this.onCreate = onCreate;
        }

        public EventEnvelope? Create(
            string streamId,
            byte[] eventData,
            string eventType,
            long eventNumber,
            GlobalEventPosition eventPosition,
            DateTime eventTime)
        {
            this.onCreate();
            return this.inner.Create(streamId, eventData, eventType, eventNumber, eventPosition, eventTime);
        }
    }

    private sealed class AlwaysThrowEnvelopeFactory : IEventEnvelopeFactory
    {
        private readonly Action onCreate;

        public AlwaysThrowEnvelopeFactory(Action onCreate)
        {
            this.onCreate = onCreate;
        }

        public EventEnvelope? Create(
            string streamId,
            byte[] eventData,
            string eventType,
            long eventNumber,
            GlobalEventPosition eventPosition,
            DateTime eventTime)
        {
            this.onCreate();
            throw new InvalidOperationException("persistent test failure");
        }
    }

    private sealed class ThrowOnSecondCreateEnvelopeFactory : IEventEnvelopeFactory
    {
        private readonly IEventEnvelopeFactory inner;
        private readonly Action onCreate;
        private int createCount;

        public ThrowOnSecondCreateEnvelopeFactory(IEventEnvelopeFactory inner, Action onCreate)
        {
            this.inner = inner;
            this.onCreate = onCreate;
        }

        public EventEnvelope? Create(
            string streamId,
            byte[] eventData,
            string eventType,
            long eventNumber,
            GlobalEventPosition eventPosition,
            DateTime eventTime)
        {
            this.onCreate();

            if (Interlocked.Increment(ref this.createCount) == 2)
            {
                throw new InvalidOperationException("transient second-event failure");
            }

            return this.inner.Create(streamId, eventData, eventType, eventNumber, eventPosition, eventTime);
        }
    }
}
