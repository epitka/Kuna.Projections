using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kurrent;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class EventEnvelopeFactoryTests
{
    private delegate bool TryResolveDelegate(Event @event, string streamId, out Guid modelId);

    [Fact]
    public void When_Deserialization_And_ModelId_Resolve_Succeed_Should_Return_Envelope()
    {
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.UtcNow;
        var @event = new TestEvent { TypeName = nameof(TestEvent), CreatedOn = createdOn, };
        var deserializer = new StubDeserializer(@event);
        var resolver = new StubResolver(
            (Event evt, string streamId, out Guid resolvedId) =>
            {
                resolvedId = modelId;
                return true;
            });

        var factory = new EventEnvelopeFactory(deserializer, resolver);

        var result = factory.Create(
            streamId: "order-1",
            eventData: Array.Empty<byte>(),
            eventType: nameof(TestEvent),
            eventNumber: 7,
            eventPosition: new GlobalEventPosition(70),
            eventTime: createdOn);

        result.HasValue.ShouldBeTrue();
        result.Value.ModelId.ShouldBe(modelId);
        result.Value.Event.ShouldBeSameAs(@event);
        result.Value.EventNumber.ShouldBe(7);
        result.Value.GlobalEventPosition.ShouldBe(new GlobalEventPosition(70));
        result.Value.StreamId.ShouldBe("order-1");
        result.Value.CreatedOn.ShouldBe(createdOn);
    }

    [Fact]
    public void When_ModelId_Cannot_Be_Resolved_Should_Return_Null()
    {
        var @event = new TestEvent { TypeName = nameof(TestEvent), CreatedOn = DateTime.UtcNow, };
        var deserializer = new StubDeserializer(@event);
        var resolver = new StubResolver(
            (Event evt, string streamId, out Guid modelId) =>
            {
                modelId = Guid.Empty;
                return false;
            });

        var factory = new EventEnvelopeFactory(deserializer, resolver);

        var result = factory.Create(
            streamId: "order-1",
            eventData: Array.Empty<byte>(),
            eventType: nameof(TestEvent),
            eventNumber: 1,
            eventPosition: new GlobalEventPosition(10),
            eventTime: DateTime.UtcNow);

        result.ShouldBeNull();
    }

    [Fact]
    public void When_Deserialization_Fails_And_ModelId_Cannot_Be_Resolved_Should_Return_Null()
    {
        var deserializer = new ThrowingDeserializer();
        var resolver = new StubResolver(
            (Event evt, string streamId, out Guid modelId) =>
            {
                modelId = Guid.Empty;
                return false;
            });

        var factory = new EventEnvelopeFactory(deserializer, resolver);

        var result = factory.Create(
            streamId: "order-1",
            eventData: Array.Empty<byte>(),
            eventType: "BrokenEvent",
            eventNumber: 2,
            eventPosition: new GlobalEventPosition(20),
            eventTime: DateTime.UtcNow);

        result.ShouldBeNull();
    }

    [Fact]
    public void When_Deserialization_Fails_Should_Return_DeserializationFailed_With_ModelId()
    {
        var deserializer = new ThrowingDeserializer();
        var modelId = Guid.NewGuid();
        var resolver = new StubResolver(
            (Event evt, string streamId, out Guid resolvedId) =>
            {
                resolvedId = modelId;
                return true;
            });

        var factory = new EventEnvelopeFactory(deserializer, resolver);

        var createdOn = DateTime.UtcNow;

        var result = factory.Create(
            streamId: "order-1",
            eventData: Array.Empty<byte>(),
            eventType: "BrokenEvent",
            eventNumber: 2,
            eventPosition: new GlobalEventPosition(20),
            eventTime: createdOn);

        result.HasValue.ShouldBeTrue();
        result.Value.ModelId.ShouldBe(modelId);
        result.Value.Event.ShouldBeOfType<DeserializationFailed>();

        var failure = (DeserializationFailed)result.Value.Event;
        failure.ModelId.ShouldBe(modelId);
        failure.EventNumber.ShouldBe(2);
        failure.GlobalEventPosition.ShouldBe(new GlobalEventPosition(20));
        failure.TypeName.ShouldBe("BrokenEvent");
    }

    private sealed class TestEvent : Event
    {
    }

    private sealed class StubDeserializer : IEventDeserializer
    {
        private readonly Event @event;

        public StubDeserializer(Event @event)
        {
            this.@event = @event;
        }

        public Event Deserialize(byte[] eventData, string eventTypeName, long globalEventNumber)
        {
            return this.@event;
        }
    }

    private sealed class ThrowingDeserializer : IEventDeserializer
    {
        public Event Deserialize(byte[] eventData, string eventTypeName, long globalEventNumber)
        {
            throw new Exception("boom");
        }
    }

    private sealed class StubResolver : IEventModelIdResolver
    {
        private readonly TryResolveDelegate tryResolve;

        public StubResolver(TryResolveDelegate tryResolve)
        {
            this.tryResolve = tryResolve;
        }

        public bool TryResolve(Event @event, string streamId, out Guid modelId)
        {
            return this.tryResolve(@event, streamId, out modelId);
        }
    }
}
