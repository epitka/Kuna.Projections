using System.Text.Json;
using FakeItEasy;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class EventDeserializerTests
{
    [Fact]
    public void Deserialize_Should_Map_By_Last_Dot_Segment()
    {
        var deserializer = CreateDeserializer();
        var data = JsonSerializer.SerializeToElement(new { name = "hello", });

        var result = deserializer.Deserialize(data, "io.kuna.test.TestEvent", "5");

        result.ShouldBeOfType<TestEvent>();
        result.TypeName.ShouldBe(nameof(TestEvent));
    }

    [Fact]
    public void Deserialize_Should_Map_Plain_Type_Name()
    {
        var deserializer = CreateDeserializer();
        var data = JsonSerializer.SerializeToElement(new { name = "hello", });

        var result = deserializer.Deserialize(data, "TestEvent", "5");

        result.ShouldBeOfType<TestEvent>();
    }

    [Fact]
    public void Deserialize_Should_Match_Type_Name_Case_Insensitively()
    {
        var deserializer = CreateDeserializer();
        var data = JsonSerializer.SerializeToElement(new { name = "hello", });

        var result = deserializer.Deserialize(data, "testevent", "5");

        result.ShouldBeOfType<TestEvent>();
    }

    [Fact]
    public void Deserialize_Should_Populate_Payload_Fields()
    {
        var deserializer = CreateDeserializer();
        var id = Guid.NewGuid();
        var data = JsonSerializer.SerializeToElement(new { name = "hello", aggregateId = id, });

        var result = deserializer.Deserialize(data, "TestEvent", "5");

        var typed = result.ShouldBeOfType<TestEvent>();
        typed.Name.ShouldBe("hello");
        typed.AggregateId.ShouldBe(id);
    }

    [Fact]
    public void Deserialize_Should_Return_UnknownEvent_When_Type_Is_Not_Mapped()
    {
        var deserializer = CreateDeserializer();
        var data = JsonSerializer.SerializeToElement(new { name = "hello", });

        var result = deserializer.Deserialize(data, "io.kuna.test.NotRegistered", "5");

        var unknown = result.ShouldBeOfType<UnknownEvent>();
        unknown.UnknownEventName.ShouldBe("io.kuna.test.NotRegistered");
    }

    [Fact]
    public void Deserialize_Should_Use_Custom_Type_Name_Resolver()
    {
        var deserializer = CreateDeserializer(_ => nameof(TestEvent));
        var data = JsonSerializer.SerializeToElement(new { name = "hello", });

        var result = deserializer.Deserialize(data, "completely.unrelated.name", "5");

        result.ShouldBeOfType<TestEvent>();
    }

    private static EventDeserializer CreateDeserializer(Func<string, string>? resolver = null)
    {
        var logger = A.Fake<ILogger<EventDeserializer>>();
        return new EventDeserializer(new[] { typeof(TestEvent), }, resolver, logger);
    }

    private sealed class TestEvent : Event
    {
        public string Name { get; set; } = string.Empty;

        public Guid AggregateId { get; set; }
    }
}
