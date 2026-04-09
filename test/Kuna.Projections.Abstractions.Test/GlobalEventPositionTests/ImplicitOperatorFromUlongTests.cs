using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class ImplicitOperatorFromUlongTests
{
    [Fact]
    public void FromUlong_Should_Set_Value()
    {
        var position = (GlobalEventPosition)42UL;

        position.Value.ShouldBe(42UL);
    }
}
