using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core.Test.Shared.Models;

public class EdgeCaseModel : Model
{
    public int UnknownEventCount { get; set; }

    public string? LastUnknownEventName { get; set; }
}
