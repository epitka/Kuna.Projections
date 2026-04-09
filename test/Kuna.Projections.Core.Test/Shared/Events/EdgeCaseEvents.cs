using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core.Test.Shared.Events;

public class EdgeCaseCreated : Event
{
    public string? Value { get; set; }
}

public class EdgeCaseThrows : Event
{
}
