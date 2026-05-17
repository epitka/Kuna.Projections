using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaContainerFixture>
{
    public const string Name = "Kafka";
}
