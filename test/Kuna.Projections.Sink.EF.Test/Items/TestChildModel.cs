using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Pipeline.EF.Test.Items;

public sealed class TestChildModel : Model
{
    public string Name { get; set; } = string.Empty;

    public IList<TestChildItem> Children { get; set; } = new List<TestChildItem>();
}
