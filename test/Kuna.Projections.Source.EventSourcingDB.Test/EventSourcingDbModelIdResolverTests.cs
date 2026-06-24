using FakeItEasy;
using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class EventSourcingDbModelIdResolverTests
{
    [Fact]
    public void Should_Resolve_From_Guid_Attribute()
    {
        var resolver = CreateResolver();
        var id = Guid.NewGuid();
        var @event = new GuidEvent { Id = id, TypeName = nameof(GuidEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "/subject/ignored", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Resolve_From_Subject_Last_Segment_When_No_Attribute()
    {
        var resolver = CreateResolver(ModelIdResolutionStrategy.UseStreamId);
        var id = Guid.NewGuid();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/orders/{id:N}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Resolve_From_Last_Segment_Of_Nested_Subject()
    {
        var resolver = CreateResolver(ModelIdResolutionStrategy.UseStreamId);
        var id = Guid.NewGuid();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/tenants/acme/orders/{id:N}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Resolve_From_Configured_Subject_Segment_Index()
    {
        var resolver = CreateResolver(ModelIdResolutionStrategy.UseStreamId, subjectSegmentIndex: 1);
        var id = Guid.NewGuid();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/orders/{id:N}/lines/3", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(id);
    }

    [Fact]
    public void Should_Return_False_When_Subject_Segment_Is_Not_A_Guid()
    {
        var resolver = CreateResolver(ModelIdResolutionStrategy.UseStreamId);
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, "/orders/not-a-guid", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Should_Use_ModelId_Attribute_And_Ignore_Subject_By_Default()
    {
        var resolver = CreateResolver();
        var attributeId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var @event = new GuidEvent { Id = attributeId, TypeName = nameof(GuidEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/orders/{subjectId:N}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(attributeId);
    }

    [Fact]
    public void Should_Return_False_When_Using_ModelId_Attribute_And_Attribute_Is_Missing()
    {
        var resolver = CreateResolver();
        var @event = new NoAttributeEvent { TypeName = nameof(NoAttributeEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/orders/{Guid.NewGuid():N}", out var modelId);

        result.ShouldBeFalse();
        modelId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Should_Use_Subject_When_Using_StreamId()
    {
        var resolver = CreateResolver(ModelIdResolutionStrategy.UseStreamId);
        var attributeId = Guid.NewGuid();
        var subjectId = Guid.NewGuid();
        var @event = new GuidEvent { Id = attributeId, TypeName = nameof(GuidEvent), CreatedOn = DateTime.UtcNow, };

        var result = resolver.TryResolve(@event, $"/orders/{subjectId:N}", out var modelId);

        result.ShouldBeTrue();
        modelId.ShouldBe(subjectId);
    }

    private static EventSourcingDbModelIdResolver CreateResolver(
        ModelIdResolutionStrategy strategy = ModelIdResolutionStrategy.UseModelIdAttribute,
        int? subjectSegmentIndex = null)
    {
        var logger = A.Fake<ILogger<EventSourcingDbModelIdResolver>>();
        return new EventSourcingDbModelIdResolver(logger, strategy, subjectSegmentIndex);
    }

    private sealed class GuidEvent : Event
    {
        [ModelId]
        public Guid Id { get; set; }
    }

    private sealed class NoAttributeEvent : Event
    {
    }
}
