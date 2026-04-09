using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Worker.Template.AccountProjection.Model;

public sealed class Account : Model
{
    public string Email { get; set; } = string.Empty;

    public bool Verified { get; set; }

    public DateTimeOffset? CreatedAt { get; set; }
}
