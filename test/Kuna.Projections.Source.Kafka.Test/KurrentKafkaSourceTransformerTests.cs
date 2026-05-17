using System.Text;
using System.Text.Json;
using Kuna.Projections.Source.Kafka;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class KurrentKafkaSourceTransformerTests
{
    [Fact]
    public void Transform_Should_Map_Kurrent_Connector_Record()
    {
        var modelId = Guid.NewGuid();
        var transformer = new KurrentKafkaSourceTransformer();

        var result = transformer.Transform(
            new KafkaSourceRecordContext
            {
                Topic = "orders-events",
                Partition = 0,
                Offset = 12,
                ValueBytes = Encoding.UTF8.GetBytes(
                    $$"""
                    {
                      "recordId": "af0d90b7-bc4a-4220-949f-4e92df3175c2",
                      "position": {
                        "streamId": "order-{{modelId:D}}",
                        "logPosition": 42123
                      },
                      "isTransformed": false,
                      "schemaInfo": {
                        "subject": "TestEvent",
                        "type": 1
                      },
                      "headers": {
                        "esdb-record-stream-revision": "7",
                        "esdb-record-timestamp": "2026-05-17T12:00:00Z"
                      },
                      "value": {
                        "value": "abc"
                      }
                    }
                    """),
                Headers = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase)
                {
                    ["esdb-record-stream-id"] = Encoding.UTF8.GetBytes($"order-{modelId:D}"),
                    ["esdb-record-stream-revision"] = Encoding.UTF8.GetBytes("7"),
                    ["esdb-record-schema-subject"] = Encoding.UTF8.GetBytes("TestEvent"),
                    ["esdb-record-timestamp"] = Encoding.UTF8.GetBytes("2026-05-17T12:00:00Z"),
                },
            });

        result.ModelId.ShouldBe(modelId);
        result.StreamId.ShouldBe($"order-{modelId:D}");
        result.EventType.ShouldBe("TestEvent");
        result.EventNumber.ShouldBe(7);

        using var payload = JsonDocument.Parse(result.EventData);
        payload.RootElement.GetProperty("value").GetString().ShouldBe("abc");
    }

    [Fact]
    public void Transform_Should_Throw_When_Stream_Id_Does_Not_Contain_Guid_Suffix()
    {
        var transformer = new KurrentKafkaSourceTransformer();

        var ex = Should.Throw<InvalidOperationException>(
            () => transformer.Transform(
                new KafkaSourceRecordContext
                {
                    Topic = "orders-events",
                    Partition = 0,
                    Offset = 12,
                    ValueBytes = Encoding.UTF8.GetBytes(
                        """
                        {
                          "position": {
                            "streamId": "order-invalid"
                          },
                          "schemaInfo": {
                            "subject": "TestEvent"
                          },
                          "headers": {
                            "esdb-record-stream-revision": "7",
                            "esdb-record-timestamp": "2026-05-17T12:00:00Z"
                          },
                          "value": {
                            "value": "abc"
                          }
                        }
                        """),
                }));

        ex.Message.ShouldContain("does not contain a Guid model id suffix");
    }
}
