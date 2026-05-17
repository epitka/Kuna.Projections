namespace Kuna.Projections.Source.Kafka;

public interface IKafkaConsumer : IDisposable
{
    IReadOnlyList<int> GetPartitions(string topic);

    void Assign(string topic, IReadOnlyCollection<int> partitions);

    void Seek(string topic, int partition, long offset);

    KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken);

    long GetHighWatermarkOffset(string topic, int partition);

    void Close();
}
