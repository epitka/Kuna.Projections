using System.Text.Json;
using EventSourcingDb;
using Kuna.Examples.Projections.Orders.Model;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Worker.EventSourcingDB_Postgres.Example.OrdersProjection;

public sealed class OrdersReplayConsistencyDiagnostics
{
    private const string SettingsSectionName = "OrdersProjection";

    private static readonly JsonSerializerOptions SnapshotJsonOptions = new()
    {
        WriteIndented = false,
    };

    private readonly OrdersDbContext dbContext;
    private readonly IClient client;
    private readonly IEventEnvelopeFactory envelopeFactory;
    private readonly EventSourcingDbSourceSettings sourceSettings;
    private readonly ILogger<OrdersReplayConsistencyDiagnostics> logger;

    public OrdersReplayConsistencyDiagnostics(
        OrdersDbContext dbContext,
        IClient client,
        IEventDeserializer eventDeserializer,
        IConfiguration configuration,
        ILogger<EventSourcingDbModelIdResolver> modelIdResolverLogger,
        ILogger<OrdersReplayConsistencyDiagnostics> logger)
    {
        this.dbContext = dbContext;
        this.client = client;

        var projectionSection = configuration.GetSection(SettingsSectionName);

        if (!projectionSection.Exists())
        {
            throw new InvalidOperationException($"Missing required configuration section: {SettingsSectionName}");
        }

        var projectionSettings = projectionSection.Get<ProjectionSettings<Order>>()
                                 ?? throw new InvalidOperationException($"Missing configuration section: {SettingsSectionName}");

        var sectionPath = $"{SettingsSectionName}:{EventSourcingDbSourceSettings.SectionName}";
        var section = configuration.GetSection(sectionPath);

        this.sourceSettings = section.Exists()
                                  ? section.Get<EventSourcingDbSourceSettings>() ?? new EventSourcingDbSourceSettings()
                                  : new EventSourcingDbSourceSettings();

        var modelIdResolver = new EventSourcingDbModelIdResolver(
            modelIdResolverLogger,
            projectionSettings.ModelIdResolutionStrategy,
            this.sourceSettings.ModelIdSubjectSegmentIndex);

        this.envelopeFactory = new EventEnvelopeFactory(eventDeserializer, modelIdResolver);
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
        var checkedCount = 0;
        ReplayConsistencyMismatch? mismatch = null;

        this.logger.LogInformation(
            "Starting replay consistency diagnostics for OrdersProjection: orderCount={OrderCount}, orderId={OrderId}, stopOnFirstMismatch={StopOnFirstMismatch}",
            orders.Count,
            request.OrderId,
            stopOnFirstMismatch);

        foreach (var dbOrder in orders)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var replayed = await this.ReplayOrderAsync(dbOrder.Id, cancellationToken);
            var dbSnapshot = ToSnapshot(dbOrder);
            var replayedSnapshot = replayed == null ? null : ToSnapshot(replayed);
            checkedCount++;

            if (replayedSnapshot == null)
            {
                mismatch = new ReplayConsistencyMismatch(
                    dbOrder.Id,
                    "Replay produced no model state for an order row that exists in the database.",
                    SerializeSnapshot(dbSnapshot),
                    null);
            }
            else if (!SnapshotsEqual(dbSnapshot, replayedSnapshot))
            {
                mismatch = new ReplayConsistencyMismatch(
                    dbOrder.Id,
                    "Persisted order row does not match per-subject replay snapshot.",
                    SerializeSnapshot(dbSnapshot),
                    SerializeSnapshot(replayedSnapshot));
            }

            if (checkedCount % logEvery == 0)
            {
                this.logger.LogInformation(
                    "Replay consistency diagnostics progress for OrdersProjection: checked={CheckedCount}/{TotalCount}",
                    checkedCount,
                    orders.Count);
            }

            if (mismatch != null && stopOnFirstMismatch)
            {
                break;
            }
        }

        var completedAt = DateTimeOffset.UtcNow;
        var result = new ReplayConsistencyResult(
            mismatch == null,
            orders.Count,
            checkedCount,
            startedAt,
            completedAt,
            (completedAt - startedAt).TotalMilliseconds,
            mismatch);

        this.logger.LogInformation(
            "Replay consistency diagnostics completed for OrdersProjection: isConsistent={IsConsistent}, checked={CheckedCount}/{TotalCount}, elapsedMs={ElapsedMs}",
            result.IsConsistent,
            result.CheckedOrders,
            result.TotalOrders,
            result.ElapsedMilliseconds);

        if (mismatch != null)
        {
            this.logger.LogWarning(
                "Replay consistency diagnostics mismatch for OrdersProjection: orderId={OrderId}, reason={Reason}",
                mismatch.OrderId,
                mismatch.Reason);
        }

        return result;
    }

    private static bool SnapshotsEqual(OrderSnapshot left, OrderSnapshot right)
    {
        if (left.Id != right.Id
            || left.EventNumber != right.EventNumber
            || left.GlobalEventPosition != right.GlobalEventPosition
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

    private static AddressSnapshot? ToSnapshot(Address? address)
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
        var query = this.dbContext.Orders
                        .AsNoTracking()
                        .Include(x => x.OrderRefunds)
                        .OrderBy(x => x.Id)
                        .AsQueryable();

        if (request.OrderId.HasValue)
        {
            query = query.Where(x => x.Id == request.OrderId.Value);
        }

        return await query.ToListAsync(cancellationToken);
    }

    private async Task<Order?> ReplayOrderAsync(Guid orderId, CancellationToken cancellationToken)
    {
        var subjectRoot = this.sourceSettings.Subject.TrimEnd('/');
        var subject = $"{subjectRoot}/{orderId:N}";
        var projection = new Kuna.Examples.Projections.Orders.OrdersProjection(orderId);
        var hadEvents = false;

        await foreach (var sourceEvent in this.client.ReadEventsAsync(
                           subject,
                           new EventSourcingDb.Types.ReadEventsOptions(Recursive: false),
                           cancellationToken))
        {
            hadEvents = true;

            var eventNumber = long.TryParse(sourceEvent.Id, out var parsed) ? parsed : 0L;

            var envelope = this.envelopeFactory.Create(
                subject: sourceEvent.Subject,
                data: sourceEvent.Data,
                eventType: sourceEvent.Type,
                eventNumber: eventNumber,
                eventPosition: new GlobalEventPosition(sourceEvent.Id),
                eventTime: sourceEvent.Time.UtcDateTime);

            if (envelope == null)
            {
                continue;
            }

            // EventSourcingDB exposes a single, globally ordered stream, so the per-order
            // events carry global (non-consecutive) ids. Use Monotonic to match the live
            // projection's configured strategy; events are read in ascending id order.
            projection.Process(envelope.Value, EventVersionCheckStrategy.Monotonic);
        }

        return hadEvents ? projection.ModelState : null;
    }
}

public sealed record ReplayConsistencyRequest(
    Guid? OrderId = null,
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

public sealed record OrderSnapshot
{
    public Guid Id { get; init; }

    public long? EventNumber { get; init; }

    public GlobalEventPosition GlobalEventPosition { get; init; }

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
