using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Sink.MongoDB.Test.Items;

public sealed class TestModel : Model
{
    public string Name { get; set; } = string.Empty;
}

public sealed class SecondaryTestModel : Model
{
    public string Code { get; set; } = string.Empty;
}
