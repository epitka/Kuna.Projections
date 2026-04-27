using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core.Test.Shared.Events;

public class ItemDeleted : Event
{
    public Guid Id { get; set; }
}
