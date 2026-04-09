using Kuna.Projections.Core.Utils;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ConstructorDelegateFactoryTests;

public class CreateConstructorFuncTests
{
    [Fact]
    public void TypeOverload_Should_Return_Null_When_Matching_Ctor_Does_Not_Exist()
    {
        var ctor = typeof(NoGuidCtorClass).CreateConstructorFunc(new[] { typeof(Guid), });

        ctor.ShouldBeNull();
    }

    [Fact]
    public void Should_Throw_When_TDelegate_Is_Not_A_Delegate_Type()
    {
        var exception = Should.Throw<ArgumentException>(ConstructorDelegateFactory.CreateConstructorFunc<object>);

        exception.Message.ShouldContain("Invoke");
    }

    [Fact]
    public void TypeGeneric_Should_Box_Value_Type_When_Return_Type_Is_Object()
    {
        var ctor = typeof(ValueThing).CreateConstructorFunc<Func<int, object>>();

        ctor.ShouldNotBeNull();

        var result = ctor(42);

        result.ShouldBeOfType<ValueThing>();
        ((ValueThing)result).Value.ShouldBe(42);
    }

    [Fact]
    public void TypeGeneric_Should_Return_Null_When_Matching_Ctor_Does_Not_Exist()
    {
        var ctor = ConstructorDelegateFactory.CreateConstructorFunc<Func<Guid, NoGuidCtorClass>>();

        ctor.ShouldBeNull();
    }

    private sealed class NoGuidCtorClass
    {
        public NoGuidCtorClass()
        {
        }
    }

    private readonly record struct ValueThing(int Value);
}
