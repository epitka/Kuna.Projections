using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Template.AccountProjection.Events;

public sealed class EmailVerified : Event
{
    [ModelId]
    public Guid AccountId { get; init; }
}
