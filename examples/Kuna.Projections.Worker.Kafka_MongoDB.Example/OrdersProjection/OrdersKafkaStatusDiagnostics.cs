using Kuna.Examples.Projections.Orders.Model;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;

namespace Kuna.Projections.Worker.Kafka_MongoDB.Example.OrdersProjection;

public sealed class OrdersKafkaStatusDiagnostics
{
    private const string SettingsSectionName = "OrdersProjection";
    private const string DatabaseName = "orders_projection";
    private const string OrdersCollectionName = "orders_order";

    private readonly IMongoCollection<Order> ordersCollection;
    private readonly ICheckpointStore checkpointStore;
    private readonly IKafkaConsumerFactory consumerFactory;
    private readonly ICheckpointSerializer<KafkaCheckpointDocument> checkpointSerializer;
    private readonly KafkaSourceSettings sourceSettings;
    private readonly ProjectionSettings<Order> projectionSettings;

    public OrdersKafkaStatusDiagnostics(
        ICheckpointStore checkpointStore,
        IKafkaConsumerFactory consumerFactory,
        ICheckpointSerializer<KafkaCheckpointDocument> checkpointSerializer,
        IConfiguration configuration)
    {
        var mongoDbConnectionString = configuration.GetConnectionString("MongoDB");

        if (string.IsNullOrWhiteSpace(mongoDbConnectionString))
        {
            throw new InvalidOperationException("Missing connection string: MongoDB");
        }

        IMongoClient mongoClient = new MongoClient(mongoDbConnectionString);
        var mongoDatabase = mongoClient.GetDatabase(DatabaseName);
        this.ordersCollection = mongoDatabase.GetCollection<Order>(OrdersCollectionName);
        this.checkpointStore = checkpointStore;
        this.consumerFactory = consumerFactory;
        this.checkpointSerializer = checkpointSerializer;

        var projectionSection = configuration.GetSection(SettingsSectionName);
        this.projectionSettings = projectionSection.Get<ProjectionSettings<Order>>()
                                  ?? throw new InvalidOperationException($"Missing configuration section: {SettingsSectionName}");

        var sectionPath = $"{SettingsSectionName}:{KafkaSourceSettings.SectionName}";
        this.sourceSettings = configuration.GetRequiredSection(sectionPath).Get<KafkaSourceSettings>()
                              ?? throw new InvalidOperationException($"Missing configuration section: {sectionPath}");
    }

    public async Task<OrdersKafkaStatusResult> RunAsync(CancellationToken cancellationToken)
    {
        var orderCount = await this.ordersCollection.CountDocumentsAsync(
            FilterDefinition<Order>.Empty,
            cancellationToken: cancellationToken);

        var checkpoint = await this.checkpointStore.GetCheckpoint(
            ProjectionModelName.For<Order>(),
            this.projectionSettings.InstanceId,
            cancellationToken);

        var checkpointDocument = this.checkpointSerializer.Deserialize(checkpoint.GlobalEventPosition);
        using var consumer = this.consumerFactory.Create(
            this.sourceSettings,
            "kuna-projections-status-ordersprojection");

        var partitions = this.ResolveAssignedPartitions(consumer, this.sourceSettings);
        var highWatermarks = partitions
                            .Select(partition => new OrdersKafkaPartitionStatus(
                                partition,
                                checkpointDocument.Partitions.TryGetValue(partition, out var currentOffset) ? currentOffset : -1,
                                consumer.GetHighWatermarkOffset(this.sourceSettings.Topic, partition)))
                            .OrderBy(x => x.Partition)
                            .ToArray();

        var totalLag = highWatermarks.Sum(x => x.Lag);

        return new OrdersKafkaStatusResult(
            Topic: this.sourceSettings.Topic,
            InstanceId: this.projectionSettings.InstanceId,
            OrderCount: orderCount,
            Checkpoint: checkpoint.GlobalEventPosition.ToString(),
            CaughtUp: totalLag == 0,
            TotalLag: totalLag,
            Partitions: highWatermarks);
    }

    private IReadOnlyList<int> ResolveAssignedPartitions(
        IKafkaConsumer consumer,
        KafkaSourceSettings sourceSettings)
    {
        var discoveredPartitions = consumer.GetPartitions(sourceSettings.Topic);

        if (discoveredPartitions.Count == 0)
        {
            throw new InvalidOperationException($"Kafka topic '{sourceSettings.Topic}' has no partitions.");
        }

        if (sourceSettings.Partitions is not { Length: > 0 })
        {
            return discoveredPartitions;
        }

        var missingPartitions = sourceSettings.Partitions.Except(discoveredPartitions).OrderBy(x => x).ToArray();

        if (missingPartitions.Length > 0)
        {
            throw new InvalidOperationException(
                $"Kafka topic '{sourceSettings.Topic}' does not contain configured partitions: {string.Join(", ", missingPartitions)}.");
        }

        return sourceSettings.Partitions.OrderBy(x => x).ToArray();
    }
}

public sealed record OrdersKafkaStatusResult(
    string Topic,
    string InstanceId,
    long OrderCount,
    string Checkpoint,
    bool CaughtUp,
    long TotalLag,
    IReadOnlyList<OrdersKafkaPartitionStatus> Partitions);

public sealed record OrdersKafkaPartitionStatus(
    int Partition,
    long CurrentOffset,
    long HighWatermark)
{
    public long Lag => Math.Max(0, this.HighWatermark - (this.CurrentOffset + 1));
}
