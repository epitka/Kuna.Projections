using Confluent.Kafka;

namespace Kuna.Projections.Source.Kafka;

internal sealed class KafkaConsumer : IKafkaConsumer
{
    private readonly IAdminClient adminClient;
    private readonly IConsumer<byte[], byte[]> consumer;

    public KafkaConsumer(
        IAdminClient adminClient,
        IConsumer<byte[], byte[]> consumer)
    {
        this.adminClient = adminClient;
        this.consumer = consumer;
    }

    public IReadOnlyList<int> GetPartitions(string topic)
    {
        var metadata = this.adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
        var topicMetadata = metadata.Topics.SingleOrDefault(x => string.Equals(x.Topic, topic, StringComparison.Ordinal));

        if (topicMetadata is null)
        {
            return [];
        }

        return topicMetadata.Partitions
                            .Select(x => x.PartitionId)
                            .OrderBy(x => x)
                            .ToArray();
    }

    public void Assign(string topic, IReadOnlyCollection<int> partitions)
    {
        this.consumer.Assign(partitions.Select(partition => new TopicPartition(topic, new Partition(partition))));
    }

    public void Seek(string topic, int partition, long offset)
    {
        this.consumer.Seek(new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset)));
    }

    public KafkaConsumedMessage? Consume(TimeSpan timeout, CancellationToken cancellationToken)
    {
        var result = this.consumer.Consume(timeout);
        return result is null ? null : KafkaConsumeResultAdapter.Adapt(result);
    }

    public long GetHighWatermarkOffset(string topic, int partition)
    {
        var watermarkOffsets = this.consumer.QueryWatermarkOffsets(
            new TopicPartition(topic, new Partition(partition)),
            TimeSpan.FromSeconds(10));

        return watermarkOffsets.High.Value;
    }

    public void Close()
    {
        this.consumer.Close();
    }

    public void Dispose()
    {
        this.adminClient.Dispose();
        this.consumer.Dispose();
    }
}
