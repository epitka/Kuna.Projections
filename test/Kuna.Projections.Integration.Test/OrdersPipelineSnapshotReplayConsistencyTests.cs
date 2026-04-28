using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core;
using Kuna.Projections.Pipeline.Integration.Test.LocalOrders;
using Kuna.Projections.Sink.EF;
using Kuna.Projections.Source.Kurrent;
using Kuna.Projections.Source.Kurrent.Extensions;
using KurrentDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Integration.Test;

[Collection(KurrentSnapshotPostgresCollection.Name)]
public class OrdersPipelineSnapshotReplayConsistencyTests
{
    private static readonly Regex OrderStreamRegex = new(@"^order-(?<id>[0-9a-fA-F\-]{36})$", RegexOptions.Compiled);

    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    private readonly KurrentDBContainerFixture kurrentFixture;
    private readonly PostgresSqlContainerFixture postgresFixture;
    private readonly ITestOutputHelper testOutput;

    public OrdersPipelineSnapshotReplayConsistencyTests(
        KurrentDBContainerFixture kurrentFixture,
        PostgresSqlContainerFixture postgresFixture,
        ITestOutputHelper testOutput)
    {
        this.kurrentFixture = kurrentFixture;
        this.postgresFixture = postgresFixture;
        this.testOutput = testOutput;
    }

    public static IEnumerable<object[]> GetReplayConsistencyCases()
    {
        yield return
        [
            new ReplayConsistencyCase(
                TargetEvents: 5000,
                MinimumCompleteOrders: 333,
                ProjectionStartOffsetEvents: 1_000,
                CatchUpStrategy: PersistenceStrategy.ModelCountBatching,
                LivePersistenceStrategy: PersistenceStrategy.ImmediateModelFlush,
                EventsBoundedCapacity: 50_000,
                ModelCountFlushThreshold: 1_000,
                LiveOrdersToAppend: 250,
                PostStartAppendDelayMs: 3_000,
                DebugProgressEnabled: true),
        ];
    }

    [Theory]
    [MemberData(nameof(GetReplayConsistencyCases))]
    public async Task Generated_Pipeline_Output_Should_Match_PerStream_Replay_For_Each_Order_Row(ReplayConsistencyCase testCase)
    {
        var startOffsetEvents = Math.Min(
            testCase.ProjectionStartOffsetEvents,
            testCase.TargetEvents);

        await this.ResetOrdersDatabase();
        await this.EnsureCheckpointExists();

        var plan = GeneratedOrderEventSeeder.BuildPlan(
            targetEvents: testCase.TargetEvents,
            minimumCompleteOrders: testCase.MinimumCompleteOrders,
            streamPrefix: "order-",
            abandonRatio: 0.20d,
            refundRatio: 0.10d,
            seed: 7331);

        var preStartEvents = plan.Events.Take(startOffsetEvents).ToList();
        var postStartEvents = plan.Events.Skip(startOffsetEvents).ToList();

        var preStartInjected = await GeneratedOrderEventSeeder.AppendAsync(
                                   this.kurrentFixture.ConnectionString,
                                   preStartEvents,
                                   cancellationToken: CancellationToken.None);

        this.LogProgress(
            $"Prepared {plan.Events.Count} events over {plan.StreamCount} streams. "
            + $"Injected pre-start {preStartInjected} events, offset={startOffsetEvents}, target={testCase.TargetEvents}.",
            testCase.DebugProgressEnabled);

        await using var provider = this.BuildServiceProvider(
            this.kurrentFixture.ConnectionString,
            this.postgresFixture.ConnectionString,
            testCase.EventsBoundedCapacity,
            testCase.ModelCountFlushThreshold,
            testCase.CatchUpStrategy,
            testCase.LivePersistenceStrategy);

        var pipeline = provider.GetRequiredService<IProjectionPipeline<Order>>();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        var pipelineTask = pipeline.RunAsync(cts.Token);
        var expectedOrderIds = ExtractExpectedOrderIds(plan.Events);

        if (testCase.PostStartAppendDelayMs > 0)
        {
            this.LogProgress($"Waiting {testCase.PostStartAppendDelayMs}ms before post-start append.", testCase.DebugProgressEnabled);
            await Task.Delay(testCase.PostStartAppendDelayMs, CancellationToken.None);
        }

        var postStartInjected = await GeneratedOrderEventSeeder.AppendAsync(
                                    this.kurrentFixture.ConnectionString,
                                    postStartEvents,
                                    cancellationToken: CancellationToken.None);

        this.LogProgress($"Injected post-start {postStartInjected} events.", testCase.DebugProgressEnabled);

        var waitForRowsTask = WaitForProjectedRowsAsync(
            this.postgresFixture.ConnectionString,
            expectedRows: plan.StreamCount,
            timeout: TimeSpan.FromMinutes(8),
            cancellationToken: cts.Token,
            progress: (current, target) => this.LogProgress($"Order rows progress: {current}/{target}", testCase.DebugProgressEnabled));

        var completedTask = await Task.WhenAny(waitForRowsTask, pipelineTask);

        if (completedTask == pipelineTask)
        {
            // Surface root cause immediately if pipeline dies before checkpoint advances.
            await pipelineTask;
            throw new InvalidOperationException("Projection pipeline completed before projected rows reached expected count.");
        }

        try
        {
            await waitForRowsTask;
        }
        catch (TimeoutException)
        {
            await this.LogMissingOrdersDiagnostics(expectedOrderIds);
            throw;
        }

        var appendedLiveOrderIds = await AppendLiveOrdersAsync(
                                       this.kurrentFixture.ConnectionString,
                                       streamPrefix: "order-",
                                       orderCount: testCase.LiveOrdersToAppend,
                                       CancellationToken.None);

        this.LogProgress($"Appended {appendedLiveOrderIds.Count} live orders after catch-up.", testCase.DebugProgressEnabled);

        await WaitForProjectedRowsAsync(
            this.postgresFixture.ConnectionString,
            expectedRows: plan.StreamCount + appendedLiveOrderIds.Count,
            timeout: TimeSpan.FromMinutes(4),
            cancellationToken: cts.Token,
            progress: (current, target) => this.LogProgress($"Live rows progress: {current}/{target}", testCase.DebugProgressEnabled));

        await cts.CancelAsync();

        try
        {
            await pipelineTask;
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // Expected on normal cancellation after rows are projected.
        }

        await this.AssertNoProjectionFailures();

        var dbOrders = await this.LoadOrdersFromDb();
        dbOrders.Count.ShouldBeGreaterThan(0);
        this.LogProgress($"Loaded {dbOrders.Count} orders from Postgres.", testCase.DebugProgressEnabled);

        var deserializer = new EventDeserializer(
            GetOrderEventTypes(),
            LoggerFactory.Create(
                             _ =>
                             {
                             })
                         .CreateLogger<EventDeserializer>());

        var resolver = new EventModelIdResolver(
            LoggerFactory.Create(
                             _ =>
                             {
                             })
                         .CreateLogger<EventModelIdResolver>());

        var envelopeFactory = new EventEnvelopeFactory(deserializer, resolver);
        await using var kurrentClient = CreateKurrentClient(this.kurrentFixture.ConnectionString);

        var processed = 0;

        foreach (var dbOrder in dbOrders)
        {
            var replayed = await ReplayOrderAsync(kurrentClient, envelopeFactory, dbOrder.Id, CancellationToken.None);
            replayed.ShouldNotBeNull();

            var expectedSnapshot = ToSnapshot(dbOrder);
            var actualSnapshot = ToSnapshot(replayed!);

            try
            {
                AssertSnapshotsEqual(expectedSnapshot, actualSnapshot);
            }
            catch (Exception)
            {
                this.LogProgress(
                    $"Snapshot mismatch for order {dbOrder.Id:D}. "
                    + $"Expected={JsonConvert.SerializeObject(expectedSnapshot)} "
                    + $"Actual={JsonConvert.SerializeObject(actualSnapshot)}",
                    testCase.DebugProgressEnabled);

                throw;
            }

            processed++;

            if (processed % 500 == 0)
            {
                this.LogProgress($"Replay compare progress: {processed}/{dbOrders.Count}.", testCase.DebugProgressEnabled);
            }
        }

        this.LogProgress($"Replay compare completed: {processed}/{dbOrders.Count}.", testCase.DebugProgressEnabled);
    }

    private static Type[] GetOrderEventTypes()
    {
        return new[]
        {
            typeof(OrderCreatedEvent),
            typeof(OrderMerchantFeesCalculatedEvent),
            typeof(OrderConfirmedEvent),
            typeof(OrderAbandondedEvent),
            typeof(RefundAppliedToOrderEvent),
        };
    }

    private static KurrentDBClient CreateKurrentClient(string connectionString)
    {
        return new KurrentDBClient(KurrentDBClientSettings.Create(connectionString));
    }

    private static OrdersDbContext CreateOrdersDbContext(string postgresConnectionString)
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
        var options = new DbContextOptionsBuilder<OrdersDbContext>()
                      .UseNpgsql(postgresConnectionString)
                      .Options;

        return new OrdersDbContext(options);
    }

    private static async Task WaitForProjectedRowsAsync(
        string postgresConnectionString,
        int expectedRows,
        TimeSpan timeout,
        CancellationToken cancellationToken,
        Action<int, int>? progress = null)
    {
        var startedAt = DateTimeOffset.UtcNow;
        var lastReported = DateTimeOffset.MinValue;
        var stableSince = DateTimeOffset.MinValue;
        var lastCount = -1;

        while (DateTimeOffset.UtcNow - startedAt < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var ctx = CreateOrdersDbContext(postgresConnectionString);
            var count = await ctx.Orders.AsNoTracking().CountAsync(cancellationToken);

            if (count >= expectedRows)
            {
                if (count != lastCount)
                {
                    lastCount = count;
                    stableSince = DateTimeOffset.UtcNow;
                }
                else if (DateTimeOffset.UtcNow - stableSince >= TimeSpan.FromSeconds(3))
                {
                    return;
                }
            }
            else
            {
                lastCount = count;
                stableSince = DateTimeOffset.MinValue;
            }

            if (progress != null
                && DateTimeOffset.UtcNow - lastReported >= TimeSpan.FromSeconds(5))
            {
                progress(count, expectedRows);
                lastReported = DateTimeOffset.UtcNow;
            }

            await Task.Delay(500, cancellationToken);
        }

        throw new TimeoutException($"Projected row count for {nameof(Order)} did not reach expected {expectedRows}.");
    }

    private static HashSet<Guid> ExtractExpectedOrderIds(IReadOnlyList<GeneratedOrderEventSeeder.PlannedEvent> events)
    {
        var ids = new HashSet<Guid>();

        foreach (var evt in events)
        {
            var match = OrderStreamRegex.Match(evt.StreamName);

            if (!match.Success)
            {
                continue;
            }

            if (Guid.TryParse(match.Groups["id"].Value, out var id))
            {
                ids.Add(id);
            }
        }

        return ids;
    }

    private static async Task<(int EventCount, int StreamCount, ulong MaxCommitPosition)> ScanOrderEventsAsync(
        string connectionString,
        string streamPrefix,
        CancellationToken cancellationToken)
    {
        using var client = CreateKurrentClient(connectionString);
        var count = 0;
        var streams = new HashSet<string>(StringComparer.Ordinal);
        ulong maxCommitPosition = 0;
        var filterOptions = new SubscriptionFilterOptions(StreamFilter.Prefix(streamPrefix));

        await using var subscription = client.SubscribeToAll(
            FromAll.Start,
            filterOptions: filterOptions,
            cancellationToken: cancellationToken);

        await foreach (var message in subscription.Messages.WithCancellation(cancellationToken))
        {
            switch (message)
            {
                case StreamMessage.Event(var resolved):
                    count++;
                    streams.Add(resolved.OriginalStreamId);

                    if (resolved.OriginalPosition.HasValue)
                    {
                        maxCommitPosition = Math.Max(maxCommitPosition, resolved.OriginalPosition.Value.CommitPosition);
                    }

                    break;
                case StreamMessage.CaughtUp:
                    return (count, streams.Count, maxCommitPosition);
            }
        }

        return (count, streams.Count, maxCommitPosition);
    }

    private static async Task<Order?> ReplayOrderAsync(
        KurrentDBClient client,
        EventEnvelopeFactory envelopeFactory,
        Guid orderId,
        CancellationToken cancellationToken)
    {
        var streamName = $"order-{orderId:D}";
        var projection = (OrdersProjection)Activator.CreateInstance(
            typeof(OrdersProjection),
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
            binder: null,
            args: new object[] { orderId, },
            culture: null)!;

        var hadEvents = false;

        await foreach (var resolved in client.ReadStreamAsync(
                           Direction.Forwards,
                           streamName,
                           StreamPosition.Start,
                           cancellationToken: cancellationToken))
        {
            hadEvents = true;
            var envelope = envelopeFactory.Create(
                streamId: resolved.Event.EventStreamId,
                eventData: resolved.Event.Data.ToArray(),
                eventType: resolved.Event.EventType,
                eventNumber: resolved.Event.EventNumber.ToInt64(),
                eventPosition: resolved.OriginalPosition.HasValue
                                   ? resolved.OriginalPosition.Value.ToGlobalEventPosition()
                                   : new GlobalEventPosition(0),
                eventTime: resolved.Event.Created);

            envelope.HasValue.ShouldBeTrue();
            projection.Process(envelope!.Value).ShouldBeTrue();
        }

        return hadEvents ? projection.ModelState : null;
    }

    private static async Task<IReadOnlyList<Guid>> AppendLiveOrdersAsync(
        string connectionString,
        string streamPrefix,
        int orderCount,
        CancellationToken cancellationToken)
    {
        using var client = CreateKurrentClient(connectionString);
        var random = new Random(7331);
        var now = DateTimeOffset.UtcNow;
        var ids = new List<Guid>(orderCount);

        for (var i = 0; i < orderCount; i++)
        {
            var orderId = Guid.NewGuid();
            ids.Add(orderId);
            var streamName = $"{streamPrefix}{orderId:D}";
            var createdAt = now.AddMilliseconds(i);
            var feesAt = createdAt.AddSeconds(5);
            var confirmedAt = createdAt.AddSeconds(10);
            var amount = decimal.Round((decimal)((random.NextDouble() * 4500d) + 10d), 2);
            var shipping = decimal.Round((decimal)(random.NextDouble() * 35d), 2);
            var tax = decimal.Round((decimal)(random.NextDouble() * 200d), 2);
            var fee = decimal.Round(Math.Max(0.01m, amount * 0.03m), 2);

            var created = new OrderCreatedEvent
            {
                Id = orderId,
                OrderNumber = $"LIVE-{i + 1:D6}",
                MerchantId = Guid.NewGuid(),
                CreatedDateTime = createdAt,
                PaymentAuthorizationId = $"live_{Guid.NewGuid():N}",
                Amount = amount,
                ShippingAmount = shipping,
                TaxAmount = tax,
                CurrencyCode = "USD",
                CustomerId = Guid.NewGuid(),
                ProductType = "physical",
                CreatedOn = createdAt.UtcDateTime,
                TypeName = nameof(OrderCreatedEvent),
            };

            var fees = new OrderMerchantFeesCalculatedEvent
            {
                OrderId = orderId,
                MerchantTransactionFeeAmount = fee,
                CreatedOn = feesAt.UtcDateTime,
                TypeName = nameof(OrderMerchantFeesCalculatedEvent),
            };

            var confirmed = new OrderConfirmedEvent
            {
                OrderId = orderId,
                CurrencyCode = "USD",
                CompletedDateTime = confirmedAt,
                CreatedOn = confirmedAt.UtcDateTime,
                TypeName = nameof(OrderConfirmedEvent),
            };

            await AppendAsync(client, streamName, created, StreamState.NoStream, cancellationToken);
            await AppendAsync(client, streamName, fees, StreamState.Any, cancellationToken);
            await AppendAsync(client, streamName, confirmed, StreamState.Any, cancellationToken);
        }

        return ids;
    }

    private static async Task AppendAsync(
        KurrentDBClient client,
        string streamName,
        Event @event,
        StreamState state,
        CancellationToken cancellationToken)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(@event, SerializerSettings));
        var eventData = new EventData(
            Uuid.NewUuid(),
            @event.GetType().Name,
            payload);

        await client.AppendToStreamAsync(
            streamName,
            state,
            new[] { eventData, },
            cancellationToken: cancellationToken);
    }

    private static OrderSnapshot ToSnapshot(Order order)
    {
        return new OrderSnapshot
        {
            Id = order.Id,
            EventNumber = order.EventNumber,
            OrderNumber = order.OrderNumber,
            OrderStatus = order.OrderStatus,
            CreatedDateTime = order.CreatedDateTime.ToUniversalTime(),
            CompletedDateTime = order.CompletedDateTime?.ToUniversalTime(),
            Amount = NormalizeDecimal(order.Amount),
            TaxAmount = order.TaxAmount.HasValue ? NormalizeDecimal(order.TaxAmount.Value) : null,
            ShippingAmount = order.ShippingAmount.HasValue ? NormalizeDecimal(order.ShippingAmount.Value) : null,
            MerchantTransactionFeeAmount = order.MerchantTransactionFeeAmount.HasValue ? NormalizeDecimal(order.MerchantTransactionFeeAmount.Value) : null,
            CustomerId = order.CustomerId,
            MerchantId = order.MerchantId,
            CurrencyCode = order.CurrencyCode,
            PaymentAuthorizationId = order.PaymentAuthorizationId,
            TotalFundsCaptured = NormalizeDecimal(order.TotalFundsCaptured),
            TotalFundsVoided = NormalizeDecimal(order.TotalFundsVoided),
            TotalFundsRefunded = NormalizeDecimal(order.TotalFundsRefunded),
            Source = order.Source,
            MerchantPlatformId = order.MerchantPlatformId,
            MerchantReference = order.MerchantReference,
            CaptureReferences = order.CaptureReferences,
            FeeReferences = order.FeeReferences,
            VoidReferences = order.VoidReferences,
            Refunds = (order.OrderRefunds ?? new List<Refund>())
                      .OrderBy(x => x.RefundId)
                      .Select(
                          x => new RefundSnapshot
                          {
                              Id = x.Id,
                              OrderId = x.OrderId,
                              RefundId = x.RefundId,
                              Amount = NormalizeDecimal(x.Amount),
                              MerchantReference = x.MerchantReference,
                              MerchantId = x.MerchantId,
                              MerchantRefundFeeRebate = NormalizeDecimal(x.MerchantRefundFeeRebate),
                              MerchantRefundTransactionFee = NormalizeDecimal(x.MerchantRefundTransactionFee),
                              MerchantRefundFeeRebatePercent = NormalizeDecimal(x.MerchantRefundFeeRebatePercent),
                              RefundDateTime = x.RefundDateTime?.ToUniversalTime(),
                          })
                      .ToList(),
        };
    }

    private static decimal NormalizeDecimal(decimal value)
    {
        return decimal.Parse(value.ToString(CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
    }

    private static string FormatToMicroseconds(DateTimeOffset value)
    {
        return value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.ffffffK", CultureInfo.InvariantCulture);
    }

    private static string? FormatToMicroseconds(DateTimeOffset? value)
    {
        return value.HasValue ? FormatToMicroseconds(value.Value) : null;
    }

    private static void AssertSnapshotsEqual(OrderSnapshot expected, OrderSnapshot actual)
    {
        expected.Id.ShouldBe(actual.Id);
        expected.EventNumber.ShouldBe(actual.EventNumber);
        expected.OrderNumber.ShouldBe(actual.OrderNumber);
        expected.OrderStatus.ShouldBe(actual.OrderStatus);
        FormatToMicroseconds(expected.CreatedDateTime).ShouldBe(FormatToMicroseconds(actual.CreatedDateTime));
        FormatToMicroseconds(expected.CompletedDateTime).ShouldBe(FormatToMicroseconds(actual.CompletedDateTime));
        expected.Amount.ShouldBe(actual.Amount);
        expected.TaxAmount.ShouldBe(actual.TaxAmount);
        expected.ShippingAmount.ShouldBe(actual.ShippingAmount);
        expected.MerchantTransactionFeeAmount.ShouldBe(actual.MerchantTransactionFeeAmount);
        expected.CustomerId.ShouldBe(actual.CustomerId);
        expected.MerchantId.ShouldBe(actual.MerchantId);
        expected.CurrencyCode.ShouldBe(actual.CurrencyCode);
        expected.PaymentAuthorizationId.ShouldBe(actual.PaymentAuthorizationId);
        expected.TotalFundsCaptured.ShouldBe(actual.TotalFundsCaptured);
        expected.TotalFundsVoided.ShouldBe(actual.TotalFundsVoided);
        expected.TotalFundsRefunded.ShouldBe(actual.TotalFundsRefunded);
        expected.Source.ShouldBe(actual.Source);
        expected.MerchantPlatformId.ShouldBe(actual.MerchantPlatformId);
        expected.MerchantReference.ShouldBe(actual.MerchantReference);
        expected.CaptureReferences.ShouldBe(actual.CaptureReferences);
        expected.FeeReferences.ShouldBe(actual.FeeReferences);
        expected.VoidReferences.ShouldBe(actual.VoidReferences);

        expected.Refunds.Count.ShouldBe(actual.Refunds.Count);

        for (var i = 0; i < expected.Refunds.Count; i++)
        {
            var e = expected.Refunds[i];
            var a = actual.Refunds[i];
            e.Id.ShouldBe(a.Id);
            e.OrderId.ShouldBe(a.OrderId);
            e.RefundId.ShouldBe(a.RefundId);
            e.Amount.ShouldBe(a.Amount);
            e.MerchantReference.ShouldBe(a.MerchantReference);
            e.MerchantId.ShouldBe(a.MerchantId);
            e.MerchantRefundFeeRebate.ShouldBe(a.MerchantRefundFeeRebate);
            e.MerchantRefundTransactionFee.ShouldBe(a.MerchantRefundTransactionFee);
            e.MerchantRefundFeeRebatePercent.ShouldBe(a.MerchantRefundFeeRebatePercent);
            FormatToMicroseconds(e.RefundDateTime).ShouldBe(FormatToMicroseconds(a.RefundDateTime));
        }
    }

    private ServiceProvider BuildServiceProvider(
        string kurrentConnectionString,
        string postgresConnectionString,
        int eventsBoundedCapacity,
        int modelCountFlushThreshold,
        PersistenceStrategy catchUpPersistenceStrategy,
        PersistenceStrategy livePersistenceStrategy)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.SetMinimumLevel(LogLevel.Warning));

        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);
        services.AddDbContext<OrdersDbContext>(
            options => options.UseNpgsql(postgresConnectionString),
            ServiceLifetime.Scoped);

        services.AddSingleton(_ => CreateKurrentClient(kurrentConnectionString));

        services.AddSingleton<IEventDeserializer>(
            sp =>
                new EventDeserializer(
                    GetOrderEventTypes(),
                    sp.GetRequiredService<ILogger<EventDeserializer>>()));

        services.AddSingleton<IProjectionSettings<Order>>(
            new ProjectionSettings<Order>
            {
                ModelIdResolutionStrategy = ModelIdResolutionStrategy.PreferAttribute,
            });

        services.AddSingleton<IProjectionEventSource<Order>>(
            sp =>
            {
                var projectionSettings = sp.GetRequiredService<IProjectionSettings<Order>>();
                var sourceSettings = new KurrentDbSourceSettings
                {
                    Filter = new KurrentDbFilterSettings
                    {
                        Kind = KurrentDbFilterKind.StreamPrefix,
                        Prefixes = ["order-",],
                    },
                };

                var modelIdResolver = new EventModelIdResolver(
                    sp.GetRequiredService<ILogger<EventModelIdResolver>>(),
                    projectionSettings.ModelIdResolutionStrategy);

                var envelopeFactory = new EventEnvelopeFactory(
                    sp.GetRequiredService<IEventDeserializer>(),
                    modelIdResolver);

                var source = new KurrentDbEventSource<Order>(
                    sp.GetRequiredService<KurrentDBClient>(),
                    envelopeFactory,
                    sourceSettings,
                    sp.GetRequiredService<ILogger<KurrentDbEventSource<Order>>>());

                return new TestProjectionEventSource<Order>(source);
            });

        services.AddSqlProjectionsDataStore<Order, OrdersDbContext>(schema: "dbo");

        var config = new ConfigurationBuilder()
                     .AddInMemoryCollection(
                         new Dictionary<string, string?>
                         {
                             ["Projections:CatchUpFlush:Strategy"] = catchUpPersistenceStrategy.ToString(),
                             ["Projections:LiveProcessingFlush:Strategy"] = livePersistenceStrategy.ToString(),
                             ["Projections:CatchUpFlush:ModelCountThreshold"] = modelCountFlushThreshold.ToString(CultureInfo.InvariantCulture),
                             ["Projections:LiveProcessingFlush:ModelCountThreshold"] = modelCountFlushThreshold.ToString(CultureInfo.InvariantCulture),
                             ["Projections:LiveProcessingFlush:Delay"] = "25",
                             ["Projections:ModelStateCacheCapacity"] = "10000",
                             ["Projections:EventVersionCheckStrategy"] = EventVersionCheckStrategy.Consecutive.ToString(),
                         })
                     .Build();

        services.AddProjection<Order>(config)
                .WithInitialEvent<OrderCreatedEvent>();

        return services.BuildServiceProvider();
    }

    private async Task ResetOrdersDatabase()
    {
        await using var ctx = CreateOrdersDbContext(this.postgresFixture.ConnectionString);
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
    }

    private async Task EnsureCheckpointExists()
    {
        await using var ctx = CreateOrdersDbContext(this.postgresFixture.ConnectionString);
        var existing = await ctx.CheckPoint.Where(x => x.ModelName == ProjectionModelName.For<Order>()).SingleOrDefaultAsync();

        if (existing == null)
        {
            ctx.CheckPoint.Add(
                new CheckPoint
                {
                    ModelName = ProjectionModelName.For<Order>(),
                    GlobalEventPosition = Position.Start.ToGlobalEventPosition(),
                });

            await ctx.SaveChangesAsync();
        }
    }

    private async Task AssertNoProjectionFailures()
    {
        await using var ctx = CreateOrdersDbContext(this.postgresFixture.ConnectionString);
        var failures = await ctx.ProjectionFailures.AsNoTracking().ToListAsync();
        failures.Count.ShouldBe(0);
    }

    private async Task<List<Order>> LoadOrdersFromDb()
    {
        await using var ctx = CreateOrdersDbContext(this.postgresFixture.ConnectionString);
        return await ctx.Orders
                        .AsNoTracking()
                        .Include(x => x.OrderRefunds)
                        .OrderBy(x => x.Id)
                        .ToListAsync();
    }

    private async Task LogMissingOrdersDiagnostics(HashSet<Guid> expectedOrderIds)
    {
        await using var ctx = CreateOrdersDbContext(this.postgresFixture.ConnectionString);

        var projectedIds = await ctx.Orders
                                    .AsNoTracking()
                                    .Select(x => x.Id)
                                    .ToListAsync();

        var projectedIdSet = projectedIds.ToHashSet();
        var missing = expectedOrderIds.Where(x => !projectedIdSet.Contains(x)).Take(50).ToList();

        var failureCount = await ctx.ProjectionFailures.AsNoTracking().CountAsync();
        var checkpoint = await ctx.CheckPoint
                                  .AsNoTracking()
                                  .Where(x => x.ModelName == ProjectionModelName.For<Order>())
                                  .Select(x => x.GlobalEventPosition)
                                  .SingleOrDefaultAsync();

        this.LogProgress(
            $"Timeout diagnostics: expectedOrders={expectedOrderIds.Count}, projectedOrders={projectedIdSet.Count}, "
            + $"missingOrders={expectedOrderIds.Count - projectedIdSet.Count}, failures={failureCount}, checkpoint={checkpoint}.",
            debugProgressEnabled: true);

        if (missing.Count > 0)
        {
            this.LogProgress("MissingOrderIds(sample): " + string.Join(",", missing.Select(x => x.ToString("D"))), debugProgressEnabled: true);
        }
    }

    private void LogProgress(string message, bool debugProgressEnabled)
    {
        if (!debugProgressEnabled)
        {
            return;
        }

        var line = $"[{DateTimeOffset.UtcNow:O}] {message}";
        this.testOutput.WriteLine(line);
    }

    private sealed class OrderSnapshot
    {
        public Guid Id { get; set; }

        public long? EventNumber { get; set; }

        public string? OrderNumber { get; set; }

        public OrderStatus OrderStatus { get; set; }

        public DateTimeOffset CreatedDateTime { get; set; }

        public DateTimeOffset? CompletedDateTime { get; set; }

        public decimal Amount { get; set; }

        public decimal? TaxAmount { get; set; }

        public decimal? ShippingAmount { get; set; }

        public decimal? MerchantTransactionFeeAmount { get; set; }

        public Guid? CustomerId { get; set; }

        public Guid MerchantId { get; set; }

        public string? CurrencyCode { get; set; }

        public string? PaymentAuthorizationId { get; set; }

        public decimal TotalFundsCaptured { get; set; }

        public decimal TotalFundsVoided { get; set; }

        public decimal TotalFundsRefunded { get; set; }

        public string? Source { get; set; }

        public string? MerchantPlatformId { get; set; }

        public string? MerchantReference { get; set; }

        public string? CaptureReferences { get; set; }

        public string? FeeReferences { get; set; }

        public string? VoidReferences { get; set; }

        public List<RefundSnapshot> Refunds { get; set; } = new();
    }

    private sealed class RefundSnapshot
    {
        public Guid Id { get; set; }

        public Guid OrderId { get; set; }

        public Guid RefundId { get; set; }

        public decimal Amount { get; set; }

        public string? MerchantReference { get; set; }

        public Guid? MerchantId { get; set; }

        public decimal? MerchantRefundFeeRebate { get; set; }

        public decimal? MerchantRefundTransactionFee { get; set; }

        public decimal? MerchantRefundFeeRebatePercent { get; set; }

        public DateTimeOffset? RefundDateTime { get; set; }
    }

    private sealed class TestProjectionEventSource<TState> : IProjectionEventSource<TState>
        where TState : class, IModel, new()
    {
        public TestProjectionEventSource(IEventSource<EventEnvelope> value)
        {
            this.Value = value;
        }

        public IEventSource<EventEnvelope> Value { get; }
    }

    public sealed record ReplayConsistencyCase(
        int TargetEvents,
        int MinimumCompleteOrders,
        int ProjectionStartOffsetEvents,
        PersistenceStrategy CatchUpStrategy,
        PersistenceStrategy LivePersistenceStrategy,
        int EventsBoundedCapacity,
        int ModelCountFlushThreshold,
        int LiveOrdersToAppend,
        int PostStartAppendDelayMs,
        bool DebugProgressEnabled);
}
