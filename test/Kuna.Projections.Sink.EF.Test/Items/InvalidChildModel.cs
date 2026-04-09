using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Pipeline.EF.Test.Items;

public sealed class InvalidChildModel : Model
{
    public string Name { get; set; } = string.Empty;

    public IList<InvalidChildItem> Children { get; set; } = new List<InvalidChildItem>();
}
