using System.Reflection;
using System.Text.Json;
using Kuna.Examples.Events;
using Kuna.Examples.Projections.Orders;
using Kuna.Examples.Projections.Orders.Model;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using ModelAddress = Kuna.Examples.Projections.Orders.Model.Address;

namespace Kuna.Projections.Worker.Kafka_MongoDB.Example.OrdersProjection;

public sealed class OrdersKafkaReplayConsistencyDiagnostics
{
    private const string SettingsSectionName = "OrdersProjection";
    private const string DatabaseName = "orders_projection";
    private const string OrdersCollectionName = "orders_order";
    private const int KafkaComparisonBatchSize = 5000;

    private static readonly JsonSerializerOptions SnapshotJsonOptions = new()
    {
        WriteIndented = false,
    };

    private readonly IMongoCollection<Order> ordersCollection;
    private readonly IKafkaConsumerFactory consumerFactory;
    private readonly ICheckpointSerializer<KafkaCheckpointDocument> checkpointSerializer;
    private readonly KafkaSourceSettings sourceSettings;
    private readonly ProjectionSettings<Order> projectionSettings;
    private readonly KafkaEventEnvelopeFactory envelopeFactory;
    private readonly ILogger<OrdersKafkaReplayConsistencyDiagnostics> logger;

    public OrdersKafkaReplayConsistencyDiagnostics(
        IKafkaConsumerFactory consumerFactory,
        ICheckpointSerializer<KafkaCheckpointDocument> checkpointSerializer,
        IConfiguration configuration,
        ILoggerFactory loggerFactory,
        ILogger<OrdersKafkaReplayConsistencyDiagnostics> logger)
    {
        var mongoDbConnectionString = configuration.GetConnectionString("MongoDB");

        if (string.IsNullOrWhiteSpace(mongoDbConnectionString))
        {
            throw new InvalidOperationException("Missing connection string: MongoDB");
        }

        IMongoClient mongoClient = new MongoClient(mongoDbConnectionString);
        var mongoDatabase = mongoClient.GetDatabase(DatabaseName);

        this.ordersCollection = mongoDatabase.GetCollection<Order>(OrdersCollectionName);
        this.consumerFactory = consumerFactory;
        this.checkpointSerializer = checkpointSerializer;
        var projectionSection = configuration.GetSection(SettingsSectionName);

        if (!projectionSection.Exists())
        {
            throw new InvalidOperationException($"Missing required configuration section: {SettingsSectionName}");
        }

        this.projectionSettings = projectionSection.Get<ProjectionSettings<Order>>()
                                  ?? throw new InvalidOperationException($"Missing configuration section: {SettingsSectionName}");

        this.sourceSettings = KafkaSourceSettingsResolver.Resolve(configuration, SettingsSectionName);

        var eventTypes = ResolveEventTypes(typeof(OrderCreated).Assembly);
        var deserializer = new KafkaEventDeserializer(eventTypes, loggerFactory.CreateLogger<KafkaEventDeserializer>());
        this.envelopeFactory = new KafkaEventEnvelopeFactory(deserializer);
        this.logger = logger;
    }

    public async Task<ReplayConsistencyResult> RunAsync(
        ReplayConsistencyRequest request,
        CancellationToken cancellationToken)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var stopOnFirstMismatch = request.StopOnFirstMismatch ?? true;
        var logEvery = Math.Max(1, request.LogEvery ?? 500);
        var orders = await this.LoadOrdersAsync(request, cancellationToken);
        var ordersById = orders.ToDictionary(x => x.Id, x => x);
        var remainingOrderIds = ordersById.Keys.ToHashSet();
        var activeProjections = new Dictionary<Guid, Kuna.Examples.Projections.Orders.OrdersProjection>();
        var touchedOrderIds = new HashSet<Guid>();
        var comparedCount = 0;
        long totalConsumedRecords = 0;
        long totalMatchedRecords = 0;
        var batchNumber = 0;
        ReplayConsistencyMismatch? mismatch = null;

        this.logger.LogInformation(
            "Starting replay consistency diagnostics for OrdersProjection (Kafka): orderCount={OrderCount}, orderId={OrderId}, limit={Limit}, stopOnFirstMismatch={StopOnFirstMismatch}",
            orders.Count,
            request.OrderId,
            request.Limit,
            stopOnFirstMismatch);

        var replaySettings = new KafkaSourceSettings
        {
            BootstrapServers = this.sourceSettings.BootstrapServers,
            Topic = this.sourceSettings.Topic,
            ClientId = this.sourceSettings.ClientId,
            ConsumerGroupId = this.sourceSettings.ConsumerGroupId,
            AutoOffsetReset = KafkaAutoOffsetReset.Earliest,
            KeyFormat = this.sourceSettings.KeyFormat,
            Transformer = this.sourceSettings.Transformer,
            Partitions = this.sourceSettings.Partitions,
            PollTimeoutMs = this.sourceSettings.PollTimeoutMs,
        };

        using var consumer = this.consumerFactory.Create(
            replaySettings,
            KafkaConsumerGroupIdResolver.ResolveReplay(
                replaySettings,
                ProjectionModelName.For<Order>(),
                this.projectionSettings.InstanceId,
                Guid.NewGuid().ToString("N")));

        var assignedPartitions = this.ResolveAssignedPartitions(consumer, replaySettings);
        var currentOffsets = assignedPartitions.ToDictionary(x => x, _ => -1L);
        var highWatermarks = assignedPartitions.ToDictionary(
            partition => partition,
            partition => consumer.GetHighWatermarkOffset(replaySettings.Topic, partition));

        consumer.Assign(replaySettings.Topic, assignedPartitions);

        this.logger.LogInformation(
            "Replay consistency diagnostics assigned Kafka partitions for OrdersProjection: topic={Topic}, partitions={Partitions}, highWatermarks={HighWatermarks}",
            replaySettings.Topic,
            string.Join(", ", assignedPartitions),
            string.Join(", ", highWatermarks.OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Value}")));

        var transformer = new NativeKafkaSourceTransformer();
        var consumedSinceComparison = 0;
        var matchedSinceComparison = 0;

        while (!cancellationToken.IsCancellationRequested
               && remainingOrderIds.Count > 0
               && !this.IsCaughtUp(assignedPartitions, currentOffsets, highWatermarks))
        {
            var message = consumer.Consume(TimeSpan.FromMilliseconds(replaySettings.PollTimeoutMs), cancellationToken);

            if (message is null)
            {
                await this.TryEvictMatchedOrdersAsync(
                    activeProjections,
                    touchedOrderIds,
                    ordersById,
                    remainingOrderIds,
                    cancellationToken);

                await Task.Yield();
                continue;
            }

            currentOffsets[message.Partition] = message.Offset;
            consumedSinceComparison++;
            totalConsumedRecords++;

            var sourceRecord = transformer.Transform(
                new KafkaSourceRecordContext
                {
                    Topic = message.Topic,
                    Partition = message.Partition,
                    Offset = message.Offset,
                    KeyBytes = message.KeyBytes,
                    ValueBytes = message.ValueBytes,
                    Headers = message.Headers,
                    TimestampUtc = message.TimestampUtc,
                });

            if (!remainingOrderIds.Contains(sourceRecord.ModelId))
            {
                continue;
            }

            matchedSinceComparison++;
            totalMatchedRecords++;

            var envelope = this.envelopeFactory.Create(
                sourceRecord,
                this.checkpointSerializer.Serialize(
                    new KafkaCheckpointDocument
                    {
                        Topic = replaySettings.Topic,
                        Partitions = currentOffsets.ToDictionary(x => x.Key, x => x.Value),
                    }));

            if (!activeProjections.TryGetValue(sourceRecord.ModelId, out var projection))
            {
                projection = new Kuna.Examples.Projections.Orders.OrdersProjection(sourceRecord.ModelId);
                activeProjections[sourceRecord.ModelId] = projection;
            }

            _ = projection.Process(envelope, this.projectionSettings.EventVersionCheckStrategy);
            _ = touchedOrderIds.Add(sourceRecord.ModelId);

            if (consumedSinceComparison < KafkaComparisonBatchSize)
            {
                continue;
            }

            batchNumber++;
            var evictedCount = await this.TryEvictMatchedOrdersAsync(
                                   activeProjections,
                                   touchedOrderIds,
                                   ordersById,
                                   remainingOrderIds,
                                   cancellationToken);

            comparedCount += evictedCount;

            if (batchNumber == 1
                || batchNumber % logEvery == 0
                || comparedCount == orders.Count)
            {
                this.logger.LogInformation(
                    "Replay consistency diagnostics progress for OrdersProjection (Kafka): batch={BatchNumber}, consumedRecords={ConsumedRecords}, batchMatchedRecords={BatchMatchedRecords}, matchedRecords={MatchedRecords}, evictedOrders={EvictedOrders}, checked={CheckedCount}/{TotalCount}, remaining={RemainingCount}, active={ActiveCount}",
                    batchNumber,
                    totalConsumedRecords,
                    matchedSinceComparison,
                    totalMatchedRecords,
                    evictedCount,
                    comparedCount,
                    orders.Count,
                    remainingOrderIds.Count,
                    activeProjections.Count);
            }

            consumedSinceComparison = 0;
            matchedSinceComparison = 0;
        }

        var trailingEvictedCount = await this.TryEvictMatchedOrdersAsync(
                                       activeProjections,
                                       touchedOrderIds,
                                       ordersById,
                                       remainingOrderIds,
                                       cancellationToken);

        comparedCount += trailingEvictedCount;

        var finalComparison = await this.TryResolveFinalMismatchAsync(
                                  activeProjections,
                                  ordersById,
                                  remainingOrderIds,
                                  stopOnFirstMismatch,
                                  cancellationToken);

        comparedCount += finalComparison.CheckedCount;
        mismatch = finalComparison.Mismatch;

        var completedAt = DateTimeOffset.UtcNow;
        var result = new ReplayConsistencyResult(
            mismatch == null,
            orders.Count,
            comparedCount,
            startedAt,
            completedAt,
            (completedAt - startedAt).TotalMilliseconds,
            mismatch);

        this.logger.LogInformation(
            "Replay consistency diagnostics completed for OrdersProjection (Kafka): isConsistent={IsConsistent}, checked={CheckedCount}/{TotalCount}, consumedRecords={ConsumedRecords}, matchedRecords={MatchedRecords}, trailingEvictedOrders={TrailingEvictedOrders}, remaining={RemainingCount}, finalOffsets={FinalOffsets}, highWatermarks={HighWatermarks}, elapsedMs={ElapsedMs}",
            result.IsConsistent,
            result.CheckedOrders,
            result.TotalOrders,
            totalConsumedRecords,
            totalMatchedRecords,
            trailingEvictedCount,
            remainingOrderIds.Count,
            string.Join(", ", currentOffsets.OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Value}")),
            string.Join(", ", highWatermarks.OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.Value}")),
            result.ElapsedMilliseconds);

        if (mismatch != null)
        {
            this.logger.LogWarning(
                "Replay consistency diagnostics mismatch for OrdersProjection (Kafka): orderId={OrderId}, reason={Reason}",
                mismatch.OrderId,
                mismatch.Reason);
        }

        return result;
    }

    private static ReplayConsistencyMismatch BuildMismatch(Order databaseOrder, Order? replayedOrder)
    {
        var dbSnapshot = ToSnapshot(databaseOrder);
        var replayedSnapshot = replayedOrder == null ? null : ToSnapshot(replayedOrder);

        return replayedSnapshot == null
                   ? new ReplayConsistencyMismatch(
                       databaseOrder.Id,
                       "Replay produced no model state for an order row that exists in the database.",
                       SerializeSnapshot(dbSnapshot),
                       null)
                   : new ReplayConsistencyMismatch(
                       databaseOrder.Id,
                       "Persisted order row does not match Kafka replay snapshot.",
                       SerializeSnapshot(dbSnapshot),
                       SerializeSnapshot(replayedSnapshot));
    }

    private static Type[] ResolveEventTypes(Assembly eventAssembly)
    {
        return eventAssembly
               .GetExportedTypes()
               .Where(x => x.IsSubclassOf(typeof(Event)))
               .ToArray();
    }

    private static bool SnapshotsEqual(OrderSnapshot left, OrderSnapshot right)
    {
        if (left.Id != right.Id
            || left.EventNumber != right.EventNumber
            || left.Amount != right.Amount
            || left.TaxAmount != right.TaxAmount
            || left.ShippingAmount != right.ShippingAmount
            || left.MerchantTransactionFeeAmount != right.MerchantTransactionFeeAmount
            || left.MerchantTransactionFeePercent != right.MerchantTransactionFeePercent
            || left.MerchantTransactionFeePercentCalculated != right.MerchantTransactionFeePercentCalculated
            || left.OrderNumber != right.OrderNumber
            || left.OrderStatus != right.OrderStatus
            || left.CreatedDateTime != right.CreatedDateTime
            || left.CompletedDateTime != right.CompletedDateTime
            || left.CustomerId != right.CustomerId
            || left.MerchantId != right.MerchantId
            || left.TotalFundsCaptured != right.TotalFundsCaptured
            || left.TotalFundsVoided != right.TotalFundsVoided
            || left.TotalFundsRefunded != right.TotalFundsRefunded
            || left.Source != right.Source
            || left.MerchantPlatformId != right.MerchantPlatformId
            || left.CurrencyCode != right.CurrencyCode
            || left.MerchantReference != right.MerchantReference
            || left.PaymentAuthorizationId != right.PaymentAuthorizationId
            || left.CaptureReferences != right.CaptureReferences
            || left.FeeReferences != right.FeeReferences
            || left.VoidReferences != right.VoidReferences)
        {
            return false;
        }

        if (!AddressSnapshotsEqual(left.ShippingAddress, right.ShippingAddress)
            || !AddressSnapshotsEqual(left.BillingAddress, right.BillingAddress)
            || !CustomerSnapshotsEqual(left.Customer, right.Customer))
        {
            return false;
        }

        if (left.Refunds.Count != right.Refunds.Count)
        {
            return false;
        }

        for (var i = 0; i < left.Refunds.Count; i++)
        {
            if (!RefundSnapshotsEqual(left.Refunds[i], right.Refunds[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static string SerializeSnapshot(OrderSnapshot snapshot)
    {
        return JsonSerializer.Serialize(snapshot, SnapshotJsonOptions);
    }

    private static decimal NormalizeDecimal(decimal value)
    {
        return decimal.Round(value, 2, MidpointRounding.AwayFromZero);
    }

    private static OrderSnapshot ToSnapshot(Order order)
    {
        return new OrderSnapshot
        {
            Id = order.Id,
            EventNumber = order.EventNumber,
            GlobalEventPosition = order.GlobalEventPosition,
            Amount = NormalizeDecimal(order.Amount),
            TaxAmount = order.TaxAmount.HasValue ? NormalizeDecimal(order.TaxAmount.Value) : null,
            ShippingAmount = order.ShippingAmount.HasValue ? NormalizeDecimal(order.ShippingAmount.Value) : null,
            MerchantTransactionFeeAmount = order.MerchantTransactionFeeAmount.HasValue ? NormalizeDecimal(order.MerchantTransactionFeeAmount.Value) : null,
            MerchantTransactionFeePercent = order.MerchantTransactionFeePercent.HasValue ? NormalizeDecimal(order.MerchantTransactionFeePercent.Value) : null,
            MerchantTransactionFeePercentCalculated = NormalizeDecimal(order.MerchantTransactionFeePercentCalculated),
            OrderNumber = order.OrderNumber,
            OrderStatus = order.OrderStatus,
            CreatedDateTime = order.CreatedDateTime.ToUniversalTime(),
            CompletedDateTime = order.CompletedDateTime?.ToUniversalTime(),
            CustomerId = order.CustomerId,
            MerchantId = order.MerchantId,
            TotalFundsCaptured = NormalizeDecimal(order.TotalFundsCaptured),
            TotalFundsVoided = NormalizeDecimal(order.TotalFundsVoided),
            TotalFundsRefunded = NormalizeDecimal(order.TotalFundsRefunded),
            Source = order.Source,
            MerchantPlatformId = order.MerchantPlatformId,
            CurrencyCode = order.CurrencyCode,
            MerchantReference = order.MerchantReference,
            PaymentAuthorizationId = order.PaymentAuthorizationId,
            CaptureReferences = order.CaptureReferences,
            FeeReferences = order.FeeReferences,
            VoidReferences = order.VoidReferences,
            ShippingAddress = ToSnapshot(order.ShippingAddress),
            BillingAddress = ToSnapshot(order.BillingAddress),
            Customer = ToSnapshot(order.Customer),
            Refunds = order.OrderRefunds
                           .OrderBy(x => x.Id)
                           .Select(ToSnapshot)
                           .ToArray(),
        };
    }

    private static RefundSnapshot ToSnapshot(Refund refund)
    {
        return new RefundSnapshot
        {
            Id = refund.Id,
            OrderId = refund.OrderId,
            Amount = NormalizeDecimal(refund.Amount),
            RefundId = refund.RefundId,
            MerchantId = refund.MerchantId,
            MerchantReference = refund.MerchantReference,
            MerchantRefundFeeRebate = NormalizeDecimal(refund.MerchantRefundFeeRebate),
            MerchantRefundFeeRebatePercent = NormalizeDecimal(refund.MerchantRefundFeeRebatePercent),
            MerchantRefundTransactionFee = NormalizeDecimal(refund.MerchantRefundTransactionFee),
            RefundDateTime = refund.RefundDateTime?.ToUniversalTime(),
        };
    }

    private static CustomerSnapshot? ToSnapshot(Customer? customer)
    {
        if (customer == null)
        {
            return null;
        }

        return new CustomerSnapshot
        {
            FirstName = customer.FirstName,
            LastName = customer.LastName,
            Email = customer.Email,
            PhoneNumber = customer.PhoneNumber,
        };
    }

    private static AddressSnapshot? ToSnapshot(ModelAddress? address)
    {
        if (address == null)
        {
            return null;
        }

        return new AddressSnapshot
        {
            Line1 = address.Line1,
            Line2 = address.Line2,
            City = address.City,
            State = address.State,
            PostCode = address.PostCode,
            Country = address.Country,
        };
    }

    private static bool AddressSnapshotsEqual(AddressSnapshot? left, AddressSnapshot? right)
    {
        if (left is null
            || right is null)
        {
            return left is null && right is null;
        }

        return left.Line1 == right.Line1
               && left.Line2 == right.Line2
               && left.City == right.City
               && left.State == right.State
               && left.PostCode == right.PostCode
               && left.Country == right.Country;
    }

    private static bool CustomerSnapshotsEqual(CustomerSnapshot? left, CustomerSnapshot? right)
    {
        if (left is null
            || right is null)
        {
            return left is null && right is null;
        }

        return left.FirstName == right.FirstName
               && left.LastName == right.LastName
               && left.Email == right.Email
               && left.PhoneNumber == right.PhoneNumber;
    }

    private static bool RefundSnapshotsEqual(RefundSnapshot left, RefundSnapshot right)
    {
        return left.Id == right.Id
               && left.OrderId == right.OrderId
               && left.Amount == right.Amount
               && left.RefundId == right.RefundId
               && left.MerchantId == right.MerchantId
               && left.MerchantReference == right.MerchantReference
               && left.MerchantRefundFeeRebate == right.MerchantRefundFeeRebate
               && left.MerchantRefundFeeRebatePercent == right.MerchantRefundFeeRebatePercent
               && left.MerchantRefundTransactionFee == right.MerchantRefundTransactionFee
               && left.RefundDateTime == right.RefundDateTime;
    }

    private async Task<List<Order>> LoadOrdersAsync(ReplayConsistencyRequest request, CancellationToken cancellationToken)
    {
        var filter = request.OrderId.HasValue
                         ? Builders<Order>.Filter.Eq(x => x.Id, request.OrderId.Value)
                         : Builders<Order>.Filter.Empty;

        IFindFluent<Order, Order> find = this.ordersCollection.Find(filter).SortBy(x => x.Id);

        if (request.Limit.HasValue)
        {
            find = find.Limit(Math.Max(1, request.Limit.Value));
        }

        return await find.ToListAsync(cancellationToken);
    }

    private async Task<int> TryEvictMatchedOrdersAsync(
        Dictionary<Guid, Kuna.Examples.Projections.Orders.OrdersProjection> activeProjections,
        HashSet<Guid> touchedOrderIds,
        IReadOnlyDictionary<Guid, Order> ordersById,
        HashSet<Guid> remainingOrderIds,
        CancellationToken cancellationToken)
    {
        if (touchedOrderIds.Count == 0)
        {
            return 0;
        }

        var touchedIds = touchedOrderIds.ToArray();
        var filter = Builders<Order>.Filter.In(x => x.Id, touchedIds);
        var persistedOrders = await this.ordersCollection.Find(filter).ToListAsync(cancellationToken);
        var persistedOrdersById = persistedOrders.ToDictionary(x => x.Id, x => x);
        var evicted = 0;

        foreach (var orderId in touchedIds)
        {
            _ = touchedOrderIds.Remove(orderId);

            if (!activeProjections.TryGetValue(orderId, out var projection)
                || !persistedOrdersById.TryGetValue(orderId, out var persistedOrder))
            {
                continue;
            }

            if (!SnapshotsEqual(ToSnapshot(persistedOrder), ToSnapshot(projection.ModelState)))
            {
                continue;
            }

            _ = activeProjections.Remove(orderId);
            _ = remainingOrderIds.Remove(orderId);
            _ = ordersById[orderId];
            evicted++;
        }

        return evicted;
    }

    private async Task<FinalComparisonResult> TryResolveFinalMismatchAsync(
        IReadOnlyDictionary<Guid, Kuna.Examples.Projections.Orders.OrdersProjection> activeProjections,
        IReadOnlyDictionary<Guid, Order> ordersById,
        IReadOnlyCollection<Guid> remainingOrderIds,
        bool stopOnFirstMismatch,
        CancellationToken cancellationToken)
    {
        if (remainingOrderIds.Count == 0)
        {
            return new FinalComparisonResult(0, null);
        }

        var unresolvedIds = remainingOrderIds.OrderBy(x => x).ToArray();
        var filter = Builders<Order>.Filter.In(x => x.Id, unresolvedIds);
        var persistedOrders = await this.ordersCollection.Find(filter).ToListAsync(cancellationToken);
        var persistedOrdersById = persistedOrders.ToDictionary(x => x.Id, x => x);
        var checkedCount = 0;
        ReplayConsistencyMismatch? firstMismatch = null;

        foreach (var orderId in unresolvedIds)
        {
            checkedCount++;

            if (!persistedOrdersById.TryGetValue(orderId, out var persistedOrder))
            {
                continue;
            }

            var replayedOrder = activeProjections.TryGetValue(orderId, out var projection)
                                    ? projection.ModelState
                                    : null;

            if (replayedOrder != null
                && SnapshotsEqual(ToSnapshot(persistedOrder), ToSnapshot(replayedOrder)))
            {
                continue;
            }

            firstMismatch = BuildMismatch(persistedOrder, replayedOrder);

            if (stopOnFirstMismatch)
            {
                break;
            }
        }

        return new FinalComparisonResult(checkedCount, firstMismatch);
    }

    private bool IsCaughtUp(
        IReadOnlyList<int> assignedPartitions,
        IReadOnlyDictionary<int, long> currentOffsets,
        IReadOnlyDictionary<int, long> highWatermarks)
    {
        foreach (var partition in assignedPartitions)
        {
            var highWatermarkOffset = highWatermarks[partition];
            var currentOffset = currentOffsets[partition];

            if (highWatermarkOffset == 0)
            {
                continue;
            }

            if (currentOffset < highWatermarkOffset - 1)
            {
                return false;
            }
        }

        return true;
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

        if (sourceSettings.Partitions is not { Length: > 0, })
        {
            return discoveredPartitions;
        }

        var missingPartitions = sourceSettings.Partitions
                                              .Except(discoveredPartitions)
                                              .OrderBy(x => x)
                                              .ToArray();

        if (missingPartitions.Length > 0)
        {
            throw new InvalidOperationException(
                $"Kafka topic '{sourceSettings.Topic}' does not contain configured partitions: {string.Join(", ", missingPartitions)}.");
        }

        return sourceSettings.Partitions.OrderBy(x => x).ToArray();
    }
}

public sealed record ReplayConsistencyRequest(
    Guid? OrderId = null,
    int? Limit = null,
    bool? StopOnFirstMismatch = null,
    int? LogEvery = null);

public sealed record ReplayConsistencyResult(
    bool IsConsistent,
    int TotalOrders,
    int CheckedOrders,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset CompletedAtUtc,
    double ElapsedMilliseconds,
    ReplayConsistencyMismatch? Mismatch);

public sealed record ReplayConsistencyMismatch(
    Guid OrderId,
    string Reason,
    string? DatabaseSnapshotJson,
    string? ReplaySnapshotJson);

public sealed record FinalComparisonResult(
    int CheckedCount,
    ReplayConsistencyMismatch? Mismatch);

public sealed record OrderSnapshot
{
    public Guid Id { get; init; }

    public long? EventNumber { get; init; }

    public GlobalEventPosition GlobalEventPosition { get; init; } = new(string.Empty);

    public decimal Amount { get; init; }

    public decimal? TaxAmount { get; init; }

    public decimal? ShippingAmount { get; init; }

    public decimal? MerchantTransactionFeeAmount { get; init; }

    public decimal? MerchantTransactionFeePercent { get; init; }

    public decimal MerchantTransactionFeePercentCalculated { get; init; }

    public string? OrderNumber { get; init; }

    public OrderStatus OrderStatus { get; init; }

    public DateTimeOffset CreatedDateTime { get; init; }

    public DateTimeOffset? CompletedDateTime { get; init; }

    public Guid? CustomerId { get; init; }

    public Guid MerchantId { get; init; }

    public decimal TotalFundsCaptured { get; init; }

    public decimal TotalFundsVoided { get; init; }

    public decimal TotalFundsRefunded { get; init; }

    public string? Source { get; init; }

    public string? MerchantPlatformId { get; init; }

    public string? CurrencyCode { get; init; }

    public string? MerchantReference { get; init; }

    public string? PaymentAuthorizationId { get; init; }

    public string? CaptureReferences { get; init; }

    public string? FeeReferences { get; init; }

    public string? VoidReferences { get; init; }

    public AddressSnapshot? ShippingAddress { get; init; }

    public AddressSnapshot? BillingAddress { get; init; }

    public CustomerSnapshot? Customer { get; init; }

    public IReadOnlyList<RefundSnapshot> Refunds { get; init; } = Array.Empty<RefundSnapshot>();
}

public sealed record AddressSnapshot
{
    public string? Line1 { get; init; }

    public string? Line2 { get; init; }

    public string? City { get; init; }

    public string? State { get; init; }

    public string? PostCode { get; init; }

    public string? Country { get; init; }
}

public sealed record CustomerSnapshot
{
    public string? FirstName { get; init; }

    public string? LastName { get; init; }

    public string? Email { get; init; }

    public string? PhoneNumber { get; init; }
}

public sealed record RefundSnapshot
{
    public Guid Id { get; init; }

    public Guid OrderId { get; init; }

    public decimal Amount { get; init; }

    public Guid RefundId { get; init; }

    public Guid MerchantId { get; init; }

    public string? MerchantReference { get; init; }

    public decimal MerchantRefundFeeRebate { get; init; }

    public decimal MerchantRefundFeeRebatePercent { get; init; }

    public decimal MerchantRefundTransactionFee { get; init; }

    public DateTimeOffset? RefundDateTime { get; init; }
}
