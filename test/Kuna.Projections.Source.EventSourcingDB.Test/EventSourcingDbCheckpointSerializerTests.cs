using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.EventSourcingDB;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public sealed class EventSourcingDbCheckpointSerializerTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("0")]
    public void Deserialize_Should_Return_Empty_For_From_Start_Sentinels(string value)
    {
        var serializer = new EventSourcingDbCheckpointSerializer();

        var result = serializer.Deserialize(new GlobalEventPosition(value));

        result.ShouldBe(string.Empty);
    }

    [Fact]
    public void Deserialize_Should_Return_Value_For_Concrete_Position()
    {
        var serializer = new EventSourcingDbCheckpointSerializer();

        var result = serializer.Deserialize(new GlobalEventPosition("42"));

        result.ShouldBe("42");
    }

    [Fact]
    public void Serialize_Should_Wrap_The_Id()
    {
        var serializer = new EventSourcingDbCheckpointSerializer();

        var result = serializer.Serialize("42");

        result.Value.ShouldBe("42");
    }
}
