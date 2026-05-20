using System.Text;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class EventEnvelopeFactoryTests
{
    [Fact]
    public void Create_Should_Return_Envelope_For_Known_Event()
    {
        var deserializer = new EventDeserializer([typeof(TestEvent),], NullLogger<EventDeserializer>.Instance);
        var factory = new EventEnvelopeFactory(deserializer);
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.Parse("2026-05-17T12:00:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind);

        var envelope = factory.Create(
            new SourceRecord
            {
                EventType = nameof(TestEvent),
                EventNumber = 7,
                ModelId = modelId,
                CreatedOn = createdOn,
                StreamId = $"order-{modelId:D}",
                EventData = Encoding.UTF8.GetBytes("""{"value":"abc"}"""),
            },
            new GlobalEventPosition("kafka-position"));

        envelope.ModelId.ShouldBe(modelId);
        envelope.EventNumber.ShouldBe(7);
        envelope.StreamId.ShouldBe($"order-{modelId:D}");
        envelope.GlobalEventPosition.ShouldBe(new GlobalEventPosition("kafka-position"));
        envelope.Event.ShouldBeOfType<TestEvent>();
        ((TestEvent)envelope.Event).Value.ShouldBe("abc");
    }

    [Fact]
    public void Create_Should_Return_DeserializationFailed_When_Payload_Is_Invalid()
    {
        var deserializer = new EventDeserializer([typeof(TestEvent),], NullLogger<EventDeserializer>.Instance);
        var factory = new EventEnvelopeFactory(deserializer);
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.Parse("2026-05-17T12:00:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind);

        var envelope = factory.Create(
            new SourceRecord
            {
                EventType = nameof(TestEvent),
                EventNumber = 7,
                ModelId = modelId,
                CreatedOn = createdOn,
                StreamId = $"order-{modelId:D}",
                EventData = Encoding.UTF8.GetBytes("""{"value":}"""),
            },
            new GlobalEventPosition("kafka-position"));

        var failure = envelope.Event.ShouldBeOfType<DeserializationFailed>();
        failure.EventNumber.ShouldBe(7);
        failure.ModelId.ShouldBe(modelId);
        failure.GlobalEventPosition.ShouldBe(new GlobalEventPosition("kafka-position"));
    }

    public sealed class TestEvent : Event
    {
        public string Value { get; init; } = string.Empty;
    }
}
