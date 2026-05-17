using System.Text;
using Kuna.Projections.Source.Kafka;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class NativeKafkaSourceTransformerTests
{
    [Fact]
    public void Transform_Should_Map_Native_Kafka_Record()
    {
        var transformer = new NativeKafkaSourceTransformer();
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.Parse("2026-05-17T12:00:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind);

        var result = transformer.Transform(
            new KafkaSourceRecordContext
            {
                Topic = "orders-events",
                Partition = 0,
                Offset = 12,
                KeyBytes = Encoding.UTF8.GetBytes(modelId.ToString("D")),
                ValueBytes = Encoding.UTF8.GetBytes("{\"value\":1}"),
                Headers = new Dictionary<string, byte[]>
                {
                    ["event-type"] = Encoding.UTF8.GetBytes("OrderCreated"),
                    ["event-number"] = Encoding.UTF8.GetBytes("3"),
                    ["created-on"] = Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                    ["stream-id"] = Encoding.UTF8.GetBytes($"order-{modelId:D}"),
                },
            });

        result.EventType.ShouldBe("OrderCreated");
        result.EventNumber.ShouldBe(3);
        result.ModelId.ShouldBe(modelId);
        result.CreatedOn.ShouldBe(createdOn);
        result.StreamId.ShouldBe($"order-{modelId:D}");
        Encoding.UTF8.GetString(result.EventData).ShouldBe("{\"value\":1}");
    }

    [Fact]
    public void Transform_Should_Fall_Back_To_Timestamp_When_CreatedOn_Header_Is_Missing()
    {
        var transformer = new NativeKafkaSourceTransformer();
        var modelId = Guid.NewGuid();
        var createdOn = DateTime.Parse("2026-05-17T12:00:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind);

        var result = transformer.Transform(
            new KafkaSourceRecordContext
            {
                Topic = "orders-events",
                Partition = 0,
                Offset = 12,
                KeyBytes = Encoding.UTF8.GetBytes(modelId.ToString("D")),
                ValueBytes = Encoding.UTF8.GetBytes("{}"),
                Headers = new Dictionary<string, byte[]>
                {
                    ["event-type"] = Encoding.UTF8.GetBytes("OrderCreated"),
                    ["event-number"] = Encoding.UTF8.GetBytes("3"),
                },
                TimestampUtc = createdOn,
            });

        result.CreatedOn.ShouldBe(createdOn);
        result.StreamId.ShouldBe($"orders-events-{modelId:D}");
    }

    [Fact]
    public void Transform_Should_Throw_When_Key_Is_Not_Guid()
    {
        var transformer = new NativeKafkaSourceTransformer();

        var ex = Should.Throw<InvalidOperationException>(
            () => transformer.Transform(
                new KafkaSourceRecordContext
                {
                    Topic = "orders-events",
                    Partition = 0,
                    Offset = 12,
                    KeyBytes = Encoding.UTF8.GetBytes("not-a-guid"),
                    ValueBytes = Encoding.UTF8.GetBytes("{}"),
                    Headers = new Dictionary<string, byte[]>
                    {
                        ["event-type"] = Encoding.UTF8.GetBytes("OrderCreated"),
                        ["event-number"] = Encoding.UTF8.GetBytes("3"),
                        ["created-on"] = Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                    },
                }));

        ex.Message.ShouldContain("not a valid Guid");
    }
}
