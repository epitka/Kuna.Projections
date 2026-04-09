using Kuna.Projections.Core.Test.Shared.Events;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.Shared.Projections;

public class ItemProjection : Projection<ItemModel>
{
    public ItemProjection(Guid modelId)
        : base(modelId)
    {
    }

    public void Apply(ItemCreated @event)
    {
        //// this.ModelState.Id = @event.Id;
        this.ModelState.Name = @event.Name;
    }

    public void Apply(ItemUpdated @event)
    {
        this.ModelState.Name = @event.Name;
    }
}
