using Kuna.Projections.Abstractions.Exceptions;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Core.Test.Shared.Events;
using Kuna.Projections.Core.Test.Shared.Models;
using Kuna.Projections.Core.Test.Shared.Projections;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionTests;

public class ProcessTests
{
    [Fact]
    public void Should_Apply_Event_And_Update_Metadata()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);
        var created = new ItemCreated
        {
            Id = modelId,
            Name = "First",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(10),
            streamId: "item-1",
            modelId: modelId,
            createdOn: created.CreatedOn,
            @event: created);

        var result = projection.Process(envelope);

        result.ShouldBeTrue();
        projection.ModelState.Name.ShouldBe("First");
        projection.ModelState.EventNumber.ShouldBe(0);
        projection.ModelState.GlobalEventPosition.ShouldBe(new GlobalEventPosition(10));
    }

    [Fact]
    public void Should_Return_False_For_Duplicate_EventNumber()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);

        var created = new ItemCreated
        {
            Id = modelId,
            Name = "First",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(10),
            streamId: "item-1",
            modelId: modelId,
            createdOn: created.CreatedOn,
            @event: created);

        projection.Process(envelope).ShouldBeTrue();

        projection.Process(envelope).ShouldBeFalse();
    }

    [Fact]
    public void Should_Throw_When_Event_Out_Of_Order()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);

        var created = new ItemCreated
        {
            Id = modelId,
            Name = "First",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var updated = new ItemUpdated
        {
            Id = modelId,
            Name = "Second",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope0 = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(10),
            streamId: "item-1",
            modelId: modelId,
            createdOn: created.CreatedOn,
            @event: created);

        var envelope2 = new EventEnvelope(
            eventNumber: 2,
            streamPosition: new GlobalEventPosition(12),
            streamId: "item-1",
            modelId: modelId,
            createdOn: updated.CreatedOn,
            @event: updated);

        projection.Process(envelope0).ShouldBeTrue();

        var ex = Should.Throw<EventOutOfOrderException>(() => projection.Process(envelope2));
        ex.ModelName.ShouldBe(ProjectionModelName.For<ItemModel>());
        ex.ExpectedEventNumber.ShouldBe(1);
        ex.ReceivedEventNumber.ShouldBe(2);
    }

    [Fact]
    public void Should_Allow_Gap_When_Monotonic_Strategy()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);

        var created = new ItemCreated
        {
            Id = modelId,
            Name = "First",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var updated = new ItemUpdated
        {
            Id = modelId,
            Name = "Second",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope0 = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(10),
            streamId: "item-1",
            modelId: modelId,
            createdOn: created.CreatedOn,
            @event: created);

        var envelope2 = new EventEnvelope(
            eventNumber: 2,
            streamPosition: new GlobalEventPosition(12),
            streamId: "item-1",
            modelId: modelId,
            createdOn: updated.CreatedOn,
            @event: updated);

        projection.Process(envelope0).ShouldBeTrue();
        projection.Process(envelope2, EventVersionCheckStrategy.Monotonic).ShouldBeTrue();
        projection.ModelState.EventNumber.ShouldBe(2);
        projection.ModelState.Name.ShouldBe("Second");
    }

    [Fact]
    public void Should_Apply_Duplicate_When_Disabled_Strategy()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);

        var created = new ItemCreated
        {
            Id = modelId,
            Name = "First",
            TypeName = nameof(ItemCreated),
            CreatedOn = DateTime.UtcNow,
        };

        var updated = new ItemUpdated
        {
            Id = modelId,
            Name = "DuplicateApplied",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope0 = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(10),
            streamId: "item-1",
            modelId: modelId,
            createdOn: created.CreatedOn,
            @event: created);

        var duplicate = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(11),
            streamId: "item-1",
            modelId: modelId,
            createdOn: updated.CreatedOn,
            @event: updated);

        projection.Process(envelope0).ShouldBeTrue();
        projection.Process(duplicate, EventVersionCheckStrategy.Disabled).ShouldBeTrue();
        projection.ModelState.EventNumber.ShouldBe(0);
        projection.ModelState.Name.ShouldBe("DuplicateApplied");
    }

    [Fact]
    public void Should_Return_False_When_Stream_Processing_Is_Faulted()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);
        projection.ModelState.EventNumber = 0;
        projection.ModelState.HasStreamProcessingFaulted = true;

        var updated = new ItemUpdated
        {
            Id = modelId,
            Name = "Ignored",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope = new EventEnvelope(
            eventNumber: 1,
            streamPosition: new GlobalEventPosition(11),
            streamId: "item-1",
            modelId: modelId,
            createdOn: updated.CreatedOn,
            @event: updated);

        projection.Process(envelope).ShouldBeFalse();
        projection.ModelState.EventNumber.ShouldBe(0);
        projection.ModelState.Name.ShouldBeNull();
    }

    [Fact]
    public void Should_Allow_Any_EventNumber_When_Model_EventNumber_Is_Null()
    {
        var modelId = Guid.NewGuid();
        var projection = new ItemProjection(modelId);
        projection.ModelState.EventNumber = null;

        var updated = new ItemUpdated
        {
            Id = modelId,
            Name = "Applied",
            TypeName = nameof(ItemUpdated),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope = new EventEnvelope(
            eventNumber: 5,
            streamPosition: new GlobalEventPosition(50),
            streamId: "item-1",
            modelId: modelId,
            createdOn: updated.CreatedOn,
            @event: updated);

        projection.Process(envelope).ShouldBeTrue();
        projection.ModelState.EventNumber.ShouldBe(5);
        projection.ModelState.GlobalEventPosition.ShouldBe(new GlobalEventPosition(50));
    }

    [Fact]
    public void Should_Dispatch_By_Event_Clr_Type_Ignoring_EventTypeName_String()
    {
        var modelId = Guid.NewGuid();
        var projection = new EdgeCaseProjection(modelId);
        var evt = new EdgeCaseCreated
        {
            TypeName = "NotRegistered",
            CreatedOn = DateTime.UtcNow,
            Value = "x",
        };

        var envelope = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(1),
            streamId: "edge-1",
            modelId: modelId,
            createdOn: evt.CreatedOn,
            @event: evt);

        projection.Process(envelope).ShouldBeTrue();
        projection.ModelState.LastUnknownEventName.ShouldBe("x");
        projection.ModelState.UnknownEventCount.ShouldBe(0);
    }

    [Fact]
    public void Should_Wrap_Handler_Exception_With_Context_And_Preserve_InnerException()
    {
        var modelId = Guid.NewGuid();
        var projection = new EdgeCaseProjection(modelId);
        var evt = new EdgeCaseThrows
        {
            TypeName = nameof(EdgeCaseThrows),
            CreatedOn = DateTime.UtcNow,
        };

        var envelope = new EventEnvelope(
            eventNumber: 0,
            streamPosition: new GlobalEventPosition(1),
            streamId: "edge-1",
            modelId: modelId,
            createdOn: evt.CreatedOn,
            @event: evt);

        var ex = Should.Throw<Exception>(() => projection.Process(envelope));
        ex.Message.ShouldContain("Apply");
        ex.Message.ShouldContain(nameof(EdgeCaseThrows));
        ex.Message.ShouldContain(nameof(EdgeCaseModel));
        ex.Message.ShouldContain(modelId.ToString());
        ex.InnerException.ShouldNotBeNull();
        ex.InnerException.ShouldBeOfType<InvalidOperationException>();
        ex.InnerException.Message.ShouldBe("edge boom");
    }
}
