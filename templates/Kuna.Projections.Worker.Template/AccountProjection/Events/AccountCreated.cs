using Kuna.Projections.Abstractions.Attributes;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Template.AccountProjection.Events;

public sealed class AccountCreated : Event
{
    [ModelId]
    public Guid AccountId { get; init; }

    public required string Email { get; init; }

    public DateTimeOffset CreatedAt { get; init; }
}
