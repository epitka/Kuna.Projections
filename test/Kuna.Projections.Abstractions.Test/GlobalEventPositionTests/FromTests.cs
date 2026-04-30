using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test.GlobalEventPositionTests;

public class FromTests
{
    [Fact]
    public void NumericString_Should_Be_Parsed()
    {
        var result = GlobalEventPosition.From("456");

        result.ShouldBe(new GlobalEventPosition(456));
    }

    [Fact]
    public void WhiteSpaceValue_Should_Throw_ArgumentException()
    {
        Should.Throw<ArgumentException>(() => GlobalEventPosition.From(" "));
    }

    [Fact]
    public void InvalidValue_Should_Throw_FormatException()
    {
        Should.Throw<FormatException>(() => GlobalEventPosition.From("not-a-number"));
    }
}
