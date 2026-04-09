using Kuna.Projections.Core;
using Kuna.Projections.Worker.Template.AccountProjection.Events;
using Kuna.Projections.Worker.Template.AccountProjection.Model;

namespace Kuna.Projections.Worker.Template.AccountProjection;

public sealed class AccountProjection : Projection<Account>
{
    public AccountProjection(Guid modelId)
        : base(modelId)
    {
    }

    public void Apply(AccountCreated @event)
    {
        this.My.Email = @event.Email;
        this.My.CreatedAt = @event.CreatedAt;
    }

    public void Apply(EmailVerified @event)
    {
        this.My.Verified = true;
    }
}
