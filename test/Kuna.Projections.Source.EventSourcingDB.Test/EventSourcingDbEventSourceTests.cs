using EventSourcingDb;
using EventSourcingDb.Types;
using FakeItEasy;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;
using EsdbEvent = EventSourcingDb.Types.Event;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class EventSourcingDbEventSourceTests
{
    [Fact]
    public void Constructor_Should_Throw_When_Strategy_Is_Consecutive()
    {
        var exception = Should.Throw<InvalidOperationException>(() => CreateSource(EventVersionCheckStrategy.Consecutive));

        exception.Message.ShouldContain("Consecutive");
        exception.Message.ShouldContain("Monotonic");
    }

    [Fact]
    public void Constructor_Should_Allow_Monotonic_Strategy()
    {
        var source = CreateSource(EventVersionCheckStrategy.Monotonic);

        source.ShouldNotBeNull();
    }

    [Fact]
    public void Constructor_Should_Allow_Disabled_Strategy()
    {
        var source = CreateSource(EventVersionCheckStrategy.Disabled);

        source.ShouldNotBeNull();
    }

    [Fact]
    public async Task ReadAll_Should_Reopen_Observe_When_It_Completes_Normally()
    {
        var client = A.Fake<IClient>();
        var observeCalls = 0;

        A.CallTo(
             () => client.ObserveEventsAsync(
                 A<string>._,
                 A<ObserveEventsOptions>._,
                 A<CancellationToken>._))
         .ReturnsLazily(
             call =>
             {
                 var callNumber = Interlocked.Increment(ref observeCalls);
                 var cancellationToken = call.GetArgument<CancellationToken>(2);
                 return callNumber == 1
                            ? CompleteImmediately()
                            : WaitUntilCancelled(cancellationToken);
             });

        var headPositionReader = A.Fake<IHeadPositionReader>();
        A.CallTo(() => headPositionReader.ReadHeadPositionAsync(A<CancellationToken>._))
         .Returns("0");

        var source = CreateSource(
            EventVersionCheckStrategy.Monotonic,
            client,
            headPositionReader);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var enumerator = source.ReadAll(new GlobalEventPosition("0"), cts.Token)
                                           .GetAsyncEnumerator(cts.Token);

        (await enumerator.MoveNextAsync()).ShouldBeTrue();
        enumerator.Current.Event.ShouldBeOfType<ProjectionCaughtUpEvent>();

        var next = enumerator.MoveNextAsync().AsTask();
        await WaitUntilAsync(() => Volatile.Read(ref observeCalls) >= 2, cts.Token);

        next.IsCompleted.ShouldBeFalse();
        await cts.CancelAsync();
        (await next).ShouldBeFalse();
    }

    private static EventSourcingDbEventSource<TestModel> CreateSource(EventVersionCheckStrategy strategy)
    {
        return CreateSource(
            strategy,
            A.Fake<IClient>(),
            A.Fake<IHeadPositionReader>());
    }

    private static EventSourcingDbEventSource<TestModel> CreateSource(
        EventVersionCheckStrategy strategy,
        IClient client,
        IHeadPositionReader headPositionReader)
    {
        return new EventSourcingDbEventSource<TestModel>(
            client,
            A.Fake<IEventEnvelopeFactory>(),
            new EventSourcingDbCheckpointSerializer(),
            headPositionReader,
            new EventSourcingDbSourceSettings(),
            new ProjectionSettings<TestModel>
            {
                InstanceId = "test",
                EventVersionCheckStrategy = strategy,
            },
            NullLogger<EventSourcingDbEventSource<TestModel>>.Instance);
    }

    private static async IAsyncEnumerable<EsdbEvent> CompleteImmediately()
    {
        await Task.CompletedTask;
        yield break;
    }

    private static async IAsyncEnumerable<EsdbEvent> WaitUntilCancelled(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        yield break;
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken cancellationToken)
    {
        while (!condition())
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
        }
    }

    private sealed class TestModel : Model
    {
    }
}
