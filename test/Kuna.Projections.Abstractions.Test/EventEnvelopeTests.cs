using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test;

public class EventEnvelopeTests
{
    [Fact]
    public void Should_Preserve_Input_Values()
    {
        var modelId = Guid.NewGuid();
        var streamPosition = new GlobalEventPosition("123");
        var createdOn = DateTime.UtcNow;
        var @event = new TestEvent { TypeName = nameof(TestEvent), CreatedOn = createdOn, };

        var envelope = new EventEnvelope(
            eventNumber: 10,
            streamPosition: streamPosition,
            streamId: "test-stream-1",
            modelId: modelId,
            createdOn: createdOn,
            @event: @event);

        envelope.EventNumber.ShouldBe(10);
        envelope.GlobalEventPosition.ShouldBe(streamPosition);
        envelope.StreamId.ShouldBe("test-stream-1");
        envelope.ModelId.ShouldBe(modelId);
        envelope.CreatedOn.ShouldBe(createdOn);
        envelope.Event.ShouldBe(@event);
    }

    private sealed class TestEvent : Event
    {
    }
}
