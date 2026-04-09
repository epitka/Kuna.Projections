using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Pipeline.EF.Test.Items;

public sealed class TestChildItem : ChildEntity
{
    public Guid Id { get; set; }

    public Guid TestChildModelId { get; set; }

    public string Value { get; set; } = string.Empty;
}
