namespace Kuna.Projections.Source.Kafka;

public sealed class KurrentKafkaSourceTransformer : IKafkaSourceTransformer
{
    public KafkaSourceRecord Transform(KafkaSourceRecordContext context)
    {
        throw new NotImplementedException("Kurrent-exported Kafka record transformation is not implemented yet.");
    }
}
