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
    public void KurrentPositionString_Should_Be_Preserved()
    {
        var result = GlobalEventPosition.From("C:76923252/P:76923252");

        result.ShouldBe(new GlobalEventPosition("C:76923252/P:76923252"));
    }

    [Fact]
    public void EmptyString_Should_Be_Preserved()
    {
        var result = GlobalEventPosition.From(string.Empty);

        result.ShouldBe(new GlobalEventPosition(string.Empty));
    }

    [Fact]
    public void WhiteSpaceValue_Should_Throw_ArgumentException()
    {
        Should.Throw<ArgumentException>(() => GlobalEventPosition.From(" "));
    }

    [Fact]
    public void NonEmptyString_Should_Be_Preserved()
    {
        var result = GlobalEventPosition.From("not-a-number");

        result.ShouldBe(new GlobalEventPosition("not-a-number"));
    }
}
