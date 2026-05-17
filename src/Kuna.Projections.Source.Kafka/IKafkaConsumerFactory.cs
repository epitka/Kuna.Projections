namespace Kuna.Projections.Source.Kafka;

public interface IKafkaConsumerFactory
{
    IKafkaConsumer Create(
        KafkaSourceSettings sourceSettings,
        string consumerGroupId);
}
