using Kuna.Projections.Core.Utils;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ConstructorDelegateFactoryTests;

public class DefaultConstructorTests
{

    [Fact]
    public void Type_Should_Return_Null_When_No_Parameterless_Ctor()
    {
        var ctor = typeof(NoDefaultCtorClass).DefaultConstructor();

        ctor.ShouldBeNull();
    }


    private sealed class NoDefaultCtorClass
    {
        public NoDefaultCtorClass(Guid id)
        {
            this.Id = id;
        }

        public Guid Id { get; }
    }
}
