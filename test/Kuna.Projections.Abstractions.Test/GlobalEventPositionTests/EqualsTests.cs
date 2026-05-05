using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class EqualsTests
{
    [Fact]
    public void SameValue_Should_Be_Equal()
    {
        var a = new GlobalEventPosition("123");
        var b = new GlobalEventPosition("123");
        var c = new GlobalEventPosition("124");

        a.ShouldBe(b);
        a.ShouldNotBe(c);
    }
}
