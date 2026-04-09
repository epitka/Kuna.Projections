using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class GetHashCodeTests
{
    [Fact]
    public void SameValue_Should_Return_Same_HashCode()
    {
        var a = new GlobalEventPosition(123);
        var b = new GlobalEventPosition(123);

        a.GetHashCode().ShouldBe(b.GetHashCode());
    }
}
