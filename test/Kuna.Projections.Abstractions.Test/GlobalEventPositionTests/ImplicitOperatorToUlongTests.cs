using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class ImplicitOperatorToUlongTests
{
    [Fact]
    public void ToUlong_Should_Return_Value()
    {
        var position = new GlobalEventPosition(99);

        ulong value = position;

        value.ShouldBe(99UL);
    }
}
