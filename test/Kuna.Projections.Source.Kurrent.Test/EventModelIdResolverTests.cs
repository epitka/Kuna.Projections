using FakeItEasy;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.Kurrent;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class EventModelIdResolverTests
{
    [Fact]
    public void Should_Resolve_From_Guid_Attribute()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new GuidEvent { Id = id, TypeName = nameof(GuidEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "stream-ignored", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Resolve_From_String_Attribute()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new StringEvent { Id = id.ToString(), TypeName = nameof(StringEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "stream-ignored", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Fallback_To_StreamId_When_No_Attribute()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Fallback_To_StreamId_When_Attribute_Is_Empty()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new StringEvent { Id = string.Empty, TypeName = nameof(StringEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Fallback_To_StreamId_When_Attribute_String_Is_Invalid_Guid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new StringEvent { Id = "not-a-guid", TypeName = nameof(StringEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Fallback_To_StreamId_When_Guid_Attribute_Is_EmptyGuid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new GuidEvent { Id = Guid.Empty, TypeName = nameof(GuidEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Return_False_When_Attribute_Type_Is_Unsupported_And_StreamId_Has_No_Guid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var @event = new UnsupportedAttributeEvent
        {
            Id = 42,
            TypeName = nameof(UnsupportedAttributeEvent),
            CreatedOn = DateTime.UtcNow,
        };

        var result = resolver.TryResolve(@event, "order-no-guid", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Should_Use_First_ModelId_Property_When_Multiple_Are_Present()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var @event = new MultiAttributeEvent
        {
            FirstId = first,
            SecondId = second,
            TypeName = nameof(MultiAttributeEvent),
            CreatedOn = DateTime.UtcNow,
        };

        var result = resolver.TryResolve(@event, "stream-ignored", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(first);
    }

    [Fact]
    public void Should_Fallback_To_StreamId_When_ModelId_Property_Has_No_Getter()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new NoGetterEvent { TypeName = nameof(NoGetterEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Return_False_When_StreamId_Does_Not_Contain_Guid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "order-without-guid", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Should_Return_False_When_StreamId_Contains_Malformed_Guid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "order-12345678-1234-1234-1234-zzzzzzzzzzzz", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Should_Return_False_When_StreamId_Contains_Extra_Text_After_Guid()
    {
        var logger = A.Fake<ILogger<EventModelIdResolver>>();
        var resolver = new EventModelIdResolver(logger);
        var id = Guid.NewGuid();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"order-{id}-extra", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    private sealed class GuidEvent : Event
    {
        [ModelId]
        public Guid Id { get; set; }
    }

    private sealed class StringEvent : Event
    {
        [ModelId]
        public string? Id { get; set; }
    }

    private sealed class NoAttributeEvent : Event
    {
    }

    private sealed class UnsupportedAttributeEvent : Event
    {
        [ModelId]
        public int Id { get; set; }
    }

    private sealed class MultiAttributeEvent : Event
    {
        [ModelId]
        public Guid FirstId { get; set; }

        [ModelId]
        public Guid SecondId { get; set; }
    }

    private sealed class NoGetterEvent : Event
    {
        [ModelId]
        public Guid Id
        {
            set
            {
            }
        }
    }
}
