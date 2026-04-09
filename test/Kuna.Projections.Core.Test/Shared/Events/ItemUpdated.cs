using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core.Test.Shared.Events;

public class ItemUpdated : Event
{
    public Guid Id { get; set; }

    public string? Name { get; set; }
}
