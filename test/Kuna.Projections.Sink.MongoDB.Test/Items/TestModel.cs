using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Sink.MongoDB.Test.Items;

public sealed class TestModel : Model
{
    public string Name { get; set; } = string.Empty;

    public Guid? ExternalId { get; set; }

    public TestChild? Child { get; set; }

    public List<TestNestedItem> Items { get; set; } = [];
}

public sealed class SecondaryTestModel : Model
{
    public string Code { get; set; } = string.Empty;
}

public sealed class TestChild
{
    public Guid CustomerId { get; set; }
}

public sealed class TestNestedItem
{
    public Guid RefundId { get; set; }
}
