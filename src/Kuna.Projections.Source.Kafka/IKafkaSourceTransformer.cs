namespace Kuna.Projections.Source.Kafka;

public interface IKafkaSourceTransformer
{
    KafkaSourceRecord Transform(KafkaSourceRecordContext context);
}
