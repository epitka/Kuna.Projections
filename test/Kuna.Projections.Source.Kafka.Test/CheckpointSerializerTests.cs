using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Source.Kafka;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class CheckpointSerializerTests
{
    [Fact]
    public void Deserialize_Should_Return_Empty_Document_For_Empty_Checkpoint()
    {
        var serializer = new CheckpointSerializer();

        var result = serializer.Deserialize(new GlobalEventPosition(string.Empty));

        result.Topic.ShouldBe(string.Empty);
        result.Partitions.ShouldBeEmpty();
    }

    [Fact]
    public void Serialize_And_Deserialize_Should_RoundTrip_Checkpoint()
    {
        var serializer = new CheckpointSerializer();
        var checkpoint = new Checkpoint
        {
            Topic = "orders-events",
            Partitions = new Dictionary<int, long>
            {
                [2] = 30,
                [0] = 10,
            },
        };

        var serialized = serializer.Serialize(checkpoint);
        var deserialized = serializer.Deserialize(serialized);

        deserialized.Topic.ShouldBe("orders-events");
        deserialized.Partitions.Count.ShouldBe(2);
        deserialized.Partitions[0].ShouldBe(10);
        deserialized.Partitions[2].ShouldBe(30);
    }

    [Fact]
    public void Deserialize_Should_Throw_For_Invalid_Checkpoint()
    {
        var serializer = new CheckpointSerializer();

        var ex = Should.Throw<FormatException>(() => serializer.Deserialize(new GlobalEventPosition("{not-json}")));

        ex.Message.ShouldContain("Invalid Kafka checkpoint");
    }
}
