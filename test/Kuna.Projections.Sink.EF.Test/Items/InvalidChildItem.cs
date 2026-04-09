namespace Kuna.Projections.Pipeline.EF.Test.Items;

public sealed class InvalidChildItem
{
    public Guid Id { get; set; }

    public Guid InvalidChildModelId { get; set; }

    public string Value { get; set; } = string.Empty;
}
