using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Core.Test.Shared.Events;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.Shared.Projections;

[InitialEvent<EdgeCaseCreated>]
public class EdgeCaseProjection : Projection<EdgeCaseModel>
{
    public EdgeCaseProjection(Guid modelId)
        : base(modelId)
    {
    }

    public void Apply(EdgeCaseCreated @event)
    {
        this.ModelState.LastUnknownEventName = @event.Value;
    }

    public void Apply(EdgeCaseThrows @event)
    {
        throw new InvalidOperationException("edge boom");
    }

    public override void Apply(UnknownEvent @event)
    {
        this.ModelState.UnknownEventCount++;
        this.ModelState.LastUnknownEventName = @event.UnknownEventName;
    }
}
