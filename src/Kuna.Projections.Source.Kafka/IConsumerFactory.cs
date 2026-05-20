using Confluent.Kafka;

namespace Kuna.Projections.Source.Kafka;

public interface IConsumerFactory
{
    IConsumer Create(
        KafkaSourceSettings sourceSettings,
        string consumerGroupId);
}

public sealed class ConsumerFactory : IConsumerFactory
{
    public IConsumer Create(
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
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = sourceSettings.BootstrapServers,
            ClientId = string.IsNullOrWhiteSpace(sourceSettings.ClientId) ? null : sourceSettings.ClientId,
            BrokerAddressFamily = BrokerAddressFamily.V4,
        };

        return new Consumer(
            new AdminClientBuilder(adminClientConfig).Build(),
            new ConsumerBuilder<byte[], byte[]>(config).Build());
    }
}
