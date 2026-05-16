using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.KurrentDB;
using KurrentDB.Client;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.KurrentDB.Test;

public sealed class KurrentDbCheckpointSerializerTests
{
    [Fact]
    public void Deserialize_Should_Return_Start_For_Empty_Checkpoint()
    {
        var serializer = new KurrentDbCheckpointSerializer();

        var result = serializer.Deserialize(new GlobalEventPosition(string.Empty));

        result.ShouldBe(Position.Start);
    }

    [Fact]
    public void Deserialize_Should_Return_Start_For_Legacy_Zero_Checkpoint()
    {
        var serializer = new KurrentDbCheckpointSerializer();

        var result = serializer.Deserialize(new GlobalEventPosition("0"));

        result.ShouldBe(Position.Start);
    }
}
