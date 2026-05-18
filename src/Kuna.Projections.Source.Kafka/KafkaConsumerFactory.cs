using Confluent.Kafka;

namespace Kuna.Projections.Source.Kafka;

public sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    public IKafkaConsumer Create(
        KafkaSourceSettings sourceSettings,
        string consumerGroupId)
    {
        ArgumentNullException.ThrowIfNull(sourceSettings);
        ArgumentException.ThrowIfNullOrWhiteSpace(consumerGroupId);

        var config = new ConsumerConfig
        {
            BootstrapServers = sourceSettings.BootstrapServers,
            GroupId = consumerGroupId,
            ClientId = string.IsNullOrWhiteSpace(sourceSettings.ClientId) ? null : sourceSettings.ClientId,
            BrokerAddressFamily = BrokerAddressFamily.V4,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = sourceSettings.AutoOffsetReset == KafkaAutoOffsetReset.Earliest
                                  ? AutoOffsetReset.Earliest
                                  : AutoOffsetReset.Latest,
        };

        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = sourceSettings.BootstrapServers,
            ClientId = string.IsNullOrWhiteSpace(sourceSettings.ClientId) ? null : sourceSettings.ClientId,
            BrokerAddressFamily = BrokerAddressFamily.V4,
        };

        return new KafkaConsumer(
            new AdminClientBuilder(adminClientConfig).Build(),
            new ConsumerBuilder<byte[], byte[]>(config).Build());
    }
}
