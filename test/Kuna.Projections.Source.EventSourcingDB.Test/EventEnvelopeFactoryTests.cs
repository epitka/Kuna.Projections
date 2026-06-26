using System.Text.Json;
using FakeItEasy;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class EventEnvelopeFactoryTests
{
    [Fact]
    public void Create_Should_Build_Envelope_With_Resolved_ModelId()
    {
        var id = Guid.NewGuid();
        var createdOn = DateTime.UtcNow;
        var deserializer = A.Fake<IEventDeserializer>();
        A.CallTo(() => deserializer.Deserialize(A<JsonElement>._, A<string>._, A<string>._))
         .Returns(new TestEvent { Id = id, TypeName = nameof(TestEvent), });

        var factory = new EventEnvelopeFactory(deserializer, CreateResolver());

        var envelope = factory.Create(
            "/orders/abc",
            JsonSerializer.SerializeToElement(new { }),
            "io.kuna.test.TestEvent",
            eventNumber: 7,
            eventPosition: new GlobalEventPosition("7"),
            eventTime: createdOn);

        envelope.ShouldNotBeNull();
        envelope.Value.ModelId.ShouldBe(id);
        envelope.Value.StreamId.ShouldBe("/orders/abc");
        envelope.Value.EventNumber.ShouldBe(7);
        envelope.Value.GlobalEventPosition.Value.ShouldBe("7");
        envelope.Value.CreatedOn.ShouldBe(createdOn);
        envelope.Value.Event.ShouldBeOfType<TestEvent>();
    }

    [Fact]
    public void Create_Should_Return_Null_When_ModelId_Cannot_Be_Resolved()
    {
        var deserializer = A.Fake<IEventDeserializer>();
        A.CallTo(() => deserializer.Deserialize(A<JsonElement>._, A<string>._, A<string>._))
         .Returns(new NoModelIdEvent { TypeName = nameof(NoModelIdEvent), });

        var factory = new EventEnvelopeFactory(
            deserializer,
            CreateResolver(ModelIdResolutionStrategy.UseStreamId));

        var envelope = factory.Create(
            "/orders/no-guid",
            JsonSerializer.SerializeToElement(new { }),
            "io.kuna.test.NoModelIdEvent",
            eventNumber: 1,
            eventPosition: new GlobalEventPosition("1"),
            eventTime: DateTime.UtcNow);

        envelope.ShouldBeNull();
    }

    [Fact]
    public void Create_Should_Wrap_Deserialization_Failure()
    {
        var id = Guid.NewGuid();
        var deserializer = A.Fake<IEventDeserializer>();
        A.CallTo(() => deserializer.Deserialize(A<JsonElement>._, A<string>._, A<string>._))
         .Throws(new JsonException("broken payload"));

        var factory = new EventEnvelopeFactory(
            deserializer,
            CreateResolver(ModelIdResolutionStrategy.UseStreamId));

        var envelope = factory.Create(
            $"/orders/{id:N}",
            JsonSerializer.SerializeToElement(new { }),
            "io.kuna.test.TestEvent",
            eventNumber: 3,
            eventPosition: new GlobalEventPosition("3"),
            eventTime: DateTime.UtcNow);

        envelope.ShouldNotBeNull();
        var failure = envelope.Value.Event.ShouldBeOfType<DeserializationFailed>();
        failure.ModelId.ShouldBe(id);
        failure.EventNumber.ShouldBe(3);
        failure.TypeName.ShouldBe("io.kuna.test.TestEvent");
    }

    private static EventSourcingDbModelIdResolver CreateResolver(ModelIdResolutionStrategy strategy = ModelIdResolutionStrategy.UseModelIdAttribute)
    {
        var logger = A.Fake<ILogger<EventSourcingDbModelIdResolver>>();
        return new EventSourcingDbModelIdResolver(logger, strategy);
    }

    private sealed class TestEvent : Event
    {
        [ModelId]
        public Guid Id { get; set; }
    }

    private sealed class NoModelIdEvent : Event
    {
    }
}
