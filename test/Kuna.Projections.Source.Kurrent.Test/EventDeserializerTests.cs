using System.Text;
using System.Text.Json;
using FakeItEasy;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.Kurrent;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class EventDeserializerTests
{
    [Fact]
    public void Deserialize_Should_Return_UnknownEvent_When_Type_Not_Registered()
    {
        var logger = A.Fake<ILogger<EventDeserializer>>();
        var sut = new EventDeserializer(Array.Empty<Type>(), logger);
        var payload = JsonSerializer.SerializeToUtf8Bytes(new { EventCreatedOn = DateTime.UtcNow, EventTypeName = "ignored", });

        var result = sut.Deserialize(payload, "NotRegistered", 10);

        result.ShouldBeOfType<UnknownEvent>();
        var unknown = (UnknownEvent)result;
        unknown.TypeName.ShouldBe(nameof(UnknownEvent));
        unknown.UnknownEventName.ShouldBe("NotRegistered");
    }

    [Fact]
    public void Deserialize_Should_Return_Known_Event_And_Set_EventTypeName()
    {
        var logger = A.Fake<ILogger<EventDeserializer>>();
        var sut = new EventDeserializer(new[] { typeof(SampleEvent), }, logger);
        var now = DateTime.UtcNow;
        var id = Guid.NewGuid();
        var payload = JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                EventCreatedOn = now,
                EventTypeName = "ignored",
                Id = id,
                Name = "abc",
            });

        var result = sut.Deserialize(payload, nameof(SampleEvent), 11);

        result.ShouldBeOfType<SampleEvent>();
        var known = (SampleEvent)result;
        known.Id.ShouldBe(id);
        known.Name.ShouldBe("abc");
        known.TypeName.ShouldBe(nameof(SampleEvent));
    }

    [Fact]
    public void Deserialize_Should_Retry_When_Utf8Bom_Is_Present()
    {
        var logger = A.Fake<ILogger<EventDeserializer>>();
        var sut = new EventDeserializer(new[] { typeof(SampleEvent), }, logger);
        var payload = JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                EventCreatedOn = DateTime.UtcNow,
                EventTypeName = "ignored",
                Id = Guid.NewGuid(),
                Name = "with-bom",
            });

        var bom = new byte[] { 0xEF, 0xBB, 0xBF, };
        var payloadWithBom = bom.Concat(payload).ToArray();

        var result = sut.Deserialize(payloadWithBom, nameof(SampleEvent), 12);

        result.ShouldBeOfType<SampleEvent>();
        ((SampleEvent)result).Name.ShouldBe("with-bom");
    }

    [Fact]
    public void Deserialize_Should_Rethrow_When_Invalid_Json_And_No_Bom()
    {
        var logger = A.Fake<ILogger<EventDeserializer>>();
        var sut = new EventDeserializer(new[] { typeof(SampleEvent), }, logger);
        var payload = Encoding.UTF8.GetBytes("{ this-is-not-json");

        Should.Throw<Exception>(() => sut.Deserialize(payload, nameof(SampleEvent), 13));
    }

    private sealed class SampleEvent : Event
    {
        public Guid Id { get; set; }

        public string? Name { get; set; }
    }
}
