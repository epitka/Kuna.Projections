using EventSourcingDb;
using FakeItEasy;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

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

    private static EventSourcingDbEventSource<TestModel> CreateSource(EventVersionCheckStrategy strategy)
    {
        return new EventSourcingDbEventSource<TestModel>(
            A.Fake<IClient>(),
            A.Fake<IEventEnvelopeFactory>(),
            new EventSourcingDbCheckpointSerializer(),
            A.Fake<IHeadPositionReader>(),
            new EventSourcingDbSourceSettings(),
            new ProjectionSettings<TestModel>
            {
                InstanceId = "test",
                EventVersionCheckStrategy = strategy,
            },
            NullLogger<EventSourcingDbEventSource<TestModel>>.Instance);
    }

    private sealed class TestModel : Model
    {
    }
}
