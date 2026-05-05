using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class ToStringTests
{
    [Fact]
    public void NumericValue_Should_Be_Returned()
    {
        var position = new GlobalEventPosition("456");

        position.ToString().ShouldBe("456");
    }
}
