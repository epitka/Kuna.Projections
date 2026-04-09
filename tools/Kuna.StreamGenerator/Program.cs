using System.Globalization;
using System.Text;
using System.Text.Json;
using Bogus;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Model;
using Kuna.StreamGenerator;
using KurrentDB.Client;

var options = GeneratorOptions.Parse(args);

if (options.CreateSnapshot)
{
    var snapshotStartedAt = DateTimeOffset.UtcNow;
    var snapshot = await global::Kuna.StreamGenerator.SnapshotWorkflow.CreateSnapshotAsync(
                       new global::Kuna.StreamGenerator.SnapshotRequest(
                           SnapshotDirectory: options.SnapshotDirectory,
                           SnapshotManifestPath: options.SnapshotManifestPath,
                           KurrentImage: options.KurrentImage,
                           ContainerDataPath: options.ContainerDataPath,
                           TargetEvents: options.TargetEvents,
                           MinimumCompleteOrders: options.MinimumCompleteOrders,
                           StreamPrefix: options.StreamPrefix,
                           AbandonRatio: options.AbandonRatio,
                           RefundRatio: options.RefundRatio,
                           Seed: options.Seed),
                       CancellationToken.None);

    var snapshotCompletedAt = DateTimeOffset.UtcNow;

    Console.WriteLine($"Snapshot created in {(snapshotCompletedAt - snapshotStartedAt).TotalSeconds:F1}s");
    Console.WriteLine($"Snapshot dir: {snapshot.SnapshotDirectory}");
    Console.WriteLine($"Snapshot manifest: {snapshot.SnapshotManifestPath}");
    Console.WriteLine($"Seeded events: {snapshot.SeedResult.TotalEvents}, orders: {snapshot.SeedResult.TotalOrders}");
    return 0;
}

if (string.IsNullOrWhiteSpace(options.ConnectionString))
{
    Console.Error.WriteLine("Missing required option: --connection-string");
    return 1;
}

var startedAt = DateTimeOffset.UtcNow;
var generator = new StreamGenerator(options);
var plan = generator.BuildPlan();

Console.WriteLine($"Planned {plan.TotalEvents} events across {plan.TotalOrders} streams.");
Console.WriteLine(
    $"Complete streams: {plan.CompleteOrderCount}, confirmed: {plan.ConfirmedCount}, abandoned: {plan.AbandonedCount}, refunds: {plan.RefundCount}.");

await WriteToKurrentAsync(plan, options);

var completedAt = DateTimeOffset.UtcNow;
var report = GenerationReport.FromPlan(plan, options, startedAt, completedAt);
await report.WriteAsync(options.ReportPath);

Console.WriteLine($"Report written to {options.ReportPath}");
return 0;

static async Task WriteToKurrentAsync(GenerationPlan plan, GeneratorOptions options)
{
    var client = new KurrentDBClient(KurrentDBClientSettings.Create(options.ConnectionString!));
    var seenStreams = new HashSet<string>(StringComparer.Ordinal);

    var stopwatch = System.Diagnostics.Stopwatch.StartNew();
    var written = 0;

    foreach (var item in plan.InterleavedEvents)
    {
        var payload = Kuna.StreamGenerator.EventStoreJson.Serialize(item.Event);
        var eventData = new EventData(
            Uuid.NewUuid(),
            item.EventTypeName,
            payload);

        var state = seenStreams.Add(item.StreamName)
                        ? StreamState.NoStream
                        : StreamState.Any;

        await client.AppendToStreamAsync(
            item.StreamName,
            state,
            new[] { eventData, },
            cancellationToken: CancellationToken.None);

        written++;

        if (written % 5000 == 0
            || written == plan.TotalEvents)
        {
            Console.WriteLine($"Written {written}/{plan.TotalEvents} events ({Math.Round(written / Math.Max(stopwatch.Elapsed.TotalSeconds, 1), 1)} ev/s)");
        }
    }

    stopwatch.Stop();
}

namespace Kuna.StreamGenerator
{
    internal sealed class StreamGenerator
    {
        private static readonly string[] CurrencyCodes = ["USD", "EUR", "GBP", "CAD",];
        private static readonly string[] ProductTypes = ["physical", "digital", "subscription",];
        private readonly GeneratorOptions options;
        private readonly Random random;
        private readonly Faker faker;

        public StreamGenerator(GeneratorOptions options)
        {
            this.options = options;
            this.random = options.Seed.HasValue ? new Random(options.Seed.Value) : Random.Shared;
            this.faker = new Faker();

            if (options.Seed.HasValue)
            {
                Randomizer.Seed = new Random(options.Seed.Value);
            }
        }

        public GenerationPlan BuildPlan()
        {
            var orders = new List<OrderPlan>(capacity: 40000);
            var totalEvents = 0;
            var nextOrderNumber = 1;

            for (var i = 0; i < this.options.MinimumCompleteOrders; i++)
            {
                var plan = this.CreateOrderPlan(nextOrderNumber++, forceComplete: true);
                orders.Add(plan);
                totalEvents += plan.Events.Count;
            }

            var remaining = this.options.TargetEvents - totalEvents;

            if (remaining < 0)
            {
                throw new InvalidOperationException(
                    $"Minimum complete orders already exceed target events. Target={this.options.TargetEvents}, planned={totalEvents}.");
            }

            while (remaining > 0)
            {
                var phase = this.ChoosePhaseForRemaining(remaining);
                var plan = this.CreateOrderPlan(nextOrderNumber++, forceComplete: false, fixedPhase: phase);
                orders.Add(plan);
                remaining -= plan.Events.Count;
            }

            var interleaved = this.Interleave(orders);
            Validate(interleaved, orders);

            return GenerationPlan.Create(orders, interleaved);
        }

        private static void Validate(IReadOnlyList<InterleavedEvent> interleaved, IReadOnlyList<OrderPlan> orders)
        {
            var totalEvents = orders.Sum(o => o.Events.Count);

            if (interleaved.Count != totalEvents)
            {
                throw new InvalidOperationException($"Interleaving lost events. Expected {totalEvents}, got {interleaved.Count}.");
            }

            var grouped = interleaved
                          .GroupBy(x => x.StreamName, StringComparer.Ordinal)
                          .ToDictionary(g => g.Key, g => g.Select(x => x.SequenceInStream).ToArray(), StringComparer.Ordinal);

            foreach (var order in orders)
            {
                if (!grouped.TryGetValue(order.StreamName, out var seqs))
                {
                    throw new InvalidOperationException($"Missing stream in interleaving: {order.StreamName}");
                }

                if (seqs.Length != order.Events.Count)
                {
                    throw new InvalidOperationException($"Event count mismatch for {order.StreamName}");
                }

                for (var i = 0; i < seqs.Length; i++)
                {
                    if (seqs[i] != i)
                    {
                        throw new InvalidOperationException($"Sequence order violated for {order.StreamName}");
                    }
                }
            }
        }

        private static Address CreateAddress(Faker faker)
        {
            return new Address
            {
                Line1 = faker.Address.StreetAddress(),
                Line2 = faker.Random.Bool(0.3f) ? faker.Address.SecondaryAddress() : null,
                City = faker.Address.City(),
                State = faker.Address.State(),
                PostCode = faker.Address.ZipCode(),
                Country = faker.Address.CountryCode(),
            };
        }

        private int ChoosePhaseForRemaining(int remaining)
        {
            var candidates = new List<int>(4);

            for (var phase = 1; phase <= 4; phase++)
            {
                if (phase <= remaining)
                {
                    candidates.Add(phase);
                }
            }

            if (remaining <= 4)
            {
                return remaining;
            }

            // Bias toward 2-4 event streams to avoid a tail of created-only streams.
            var weighted = new List<int>(16);

            foreach (var candidate in candidates)
            {
                var weight = candidate switch
                             {
                                 1 => 2,
                                 2 => 5,
                                 3 => 6,
                                 4 => 3,
                                 _ => 1,
                             };

                for (var i = 0; i < weight; i++)
                {
                    weighted.Add(candidate);
                }
            }

            return weighted[this.random.Next(weighted.Count)];
        }

        private OrderPlan CreateOrderPlan(int orderSequence, bool forceComplete, int? fixedPhase = null)
        {
            var orderId = Guid.NewGuid();
            var streamName = $"{this.options.StreamPrefix}{orderId:D}";
            var isAbandoned = this.random.NextDouble() < this.options.AbandonRatio;
            var isConfirmed = !isAbandoned;
            var includeRefund = isConfirmed && this.random.NextDouble() < this.options.RefundRatio;

            var targetPhase = fixedPhase ?? (includeRefund ? 4 : 3);

            if (forceComplete && targetPhase < 3)
            {
                targetPhase = includeRefund ? 4 : 3;
            }

            if (targetPhase == 4)
            {
                isAbandoned = false;
                isConfirmed = true;
                includeRefund = true;
            }
            else if (targetPhase < 4)
            {
                includeRefund = false;
            }

            var createdAt = this.RandomCreatedAt();
            var feeAt = createdAt.AddMinutes(this.random.Next(1, 90));
            var terminalAt = feeAt.AddMinutes(this.random.Next(1, 180));
            var refundAt = terminalAt.AddMinutes(this.random.Next(1, 240));

            var amount = decimal.Round((decimal)this.faker.Random.Double(5, 5000), 2);
            var shipping = this.faker.Random.Bool(0.65f)
                               ? decimal.Round((decimal)this.faker.Random.Double(0, 50), 2)
                               : (decimal?)null;

            var tax = this.faker.Random.Bool(0.8f)
                          ? decimal.Round((decimal)this.faker.Random.Double(0, 200), 2)
                          : (decimal?)null;

            var currencyCode = this.faker.PickRandom(CurrencyCodes);
            var customer = this.faker.Person;
            var billingAddress = CreateAddress(this.faker);
            var postalAddress = this.faker.Random.Bool(0.7f) ? billingAddress : CreateAddress(this.faker);

            var events = new List<EventEnvelopeSeed>(4);
            var created = new OrderCreatedEvent
            {
                Id = orderId,
                OrderNumber = $"ORD-{orderSequence:D7}",
                MerchantId = Guid.NewGuid(),
                CreatedDateTime = createdAt,
                MerchantReference = $"merchant-ref-{this.faker.Random.AlphaNumeric(12)}",
                Email = customer.Email,
                FirstNames = customer.FirstName,
                LastName = customer.LastName,
                Phone = customer.Phone,
                BillingAddress = billingAddress,
                PostalAddress = postalAddress,
                PaymentAuthorizationId = $"payauth_{this.faker.Random.AlphaNumeric(16)}",
                Amount = amount,
                ShippingAmount = shipping,
                TaxAmount = tax,
                CurrencyCode = currencyCode,
                CustomerId = this.faker.Random.Bool(0.8f) ? Guid.NewGuid() : null,
                Source = this.faker.PickRandom("web", "mobile", "store"),
                ProductType = this.faker.PickRandom(ProductTypes),
                CreatedOn = createdAt.UtcDateTime,
                TypeName = nameof(OrderCreatedEvent),
            };

            events.Add(new EventEnvelopeSeed(streamName, 0, created));

            if (targetPhase >= 2)
            {
                var fee = decimal.Round(Math.Max(0.01m, amount * decimal.Round((decimal)this.faker.Random.Double(0.01, 0.08), 4)), 2);
                var feesCalculated = new OrderMerchantFeesCalculatedEvent
                {
                    OrderId = orderId,
                    MerchantTransactionFeeAmount = fee,
                    CreatedOn = feeAt.UtcDateTime,
                    TypeName = nameof(OrderMerchantFeesCalculatedEvent),
                };

                events.Add(new EventEnvelopeSeed(streamName, 1, feesCalculated));
            }

            if (targetPhase >= 3)
            {
                Event terminal = isConfirmed
                                     ? new OrderConfirmedEvent
                                     {
                                         OrderId = orderId,
                                         CurrencyCode = currencyCode,
                                         CompletedDateTime = terminalAt,
                                         CreatedOn = terminalAt.UtcDateTime,
                                         TypeName = nameof(OrderConfirmedEvent),
                                     }
                                     : new OrderAbandondedEvent
                                     {
                                         OrderId = orderId,
                                         CompletedDateTime = terminalAt,
                                         CreatedOn = terminalAt.UtcDateTime,
                                         TypeName = nameof(OrderAbandondedEvent),
                                     };

                events.Add(new EventEnvelopeSeed(streamName, 2, terminal));
            }

            if (targetPhase >= 4)
            {
                var refund = new RefundAppliedToOrderEvent
                {
                    OrderId = orderId,
                    Amount = 1.00m,
                    RefundId = Guid.NewGuid(),
                    MerchantReference = $"rfnd_{this.faker.Random.AlphaNumeric(12)}",
                    CreatedOn = refundAt.UtcDateTime,
                    TypeName = nameof(RefundAppliedToOrderEvent),
                };

                events.Add(new EventEnvelopeSeed(streamName, 3, refund));
            }

            return new OrderPlan(
                streamName,
                orderId,
                IsComplete: targetPhase >= 3,
                IsConfirmed: targetPhase >= 3 && isConfirmed,
                IsAbandoned: targetPhase >= 3 && isAbandoned,
                HasRefund: targetPhase >= 4,
                events);
        }

        private DateTimeOffset RandomCreatedAt()
        {
            // "Realistic" default: within the last 90 days.
            var now = DateTimeOffset.UtcNow;
            var windowStart = now.AddDays(-90);
            var totalSeconds = (int)(now - windowStart).TotalSeconds;
            var offsetSeconds = this.random.Next(0, Math.Max(totalSeconds, 1));
            return windowStart.AddSeconds(offsetSeconds);
        }

        private List<InterleavedEvent> Interleave(IReadOnlyList<OrderPlan> orders)
        {
            var states = orders
                         .Select(order => new OrderCursor(order))
                         .ToList();

            var active = Enumerable.Range(0, states.Count).ToList();
            var result = new List<InterleavedEvent>(orders.Sum(o => o.Events.Count));
            var lastChosen = -1;

            while (active.Count > 0)
            {
                var pickIndex = this.random.Next(active.Count);

                if (active.Count > 1
                    && active[pickIndex] == lastChosen)
                {
                    pickIndex = (pickIndex + 1) % active.Count;
                }

                var stateIndex = active[pickIndex];
                var cursor = states[stateIndex];
                var next = cursor.Next();
                result.Add(new InterleavedEvent(cursor.Order.StreamName, next.Event, next.SequenceInStream));
                lastChosen = stateIndex;

                if (!cursor.HasNext)
                {
                    active.RemoveAt(pickIndex);
                }
            }

            return result;
        }
    }

    internal sealed record OrderPlan(
        string StreamName,
        Guid OrderId,
        bool IsComplete,
        bool IsConfirmed,
        bool IsAbandoned,
        bool HasRefund,
        IReadOnlyList<EventEnvelopeSeed> Events);

    internal sealed record EventEnvelopeSeed(string StreamName, int SequenceInStream, Event Event)
    {
        public string EventTypeName => this.Event.TypeName;
    }

    internal sealed record InterleavedEvent(string StreamName, Event Event, int SequenceInStream)
    {
        public string EventTypeName => this.Event.TypeName;
    }

    internal sealed class OrderCursor
    {
        private int index;

        public OrderCursor(OrderPlan order)
        {
            this.Order = order;
        }

        public OrderPlan Order { get; }

        public bool HasNext => this.index < this.Order.Events.Count;

        public EventEnvelopeSeed Next()
        {
            if (!this.HasNext)
            {
                throw new InvalidOperationException("No more events in cursor.");
            }

            return this.Order.Events[this.index++];
        }
    }

    internal sealed class GenerationPlan
    {
        private GenerationPlan(
            IReadOnlyList<OrderPlan> orders,
            IReadOnlyList<InterleavedEvent> interleavedEvents)
        {
            this.Orders = orders;
            this.InterleavedEvents = interleavedEvents;
            this.TotalOrders = orders.Count;
            this.TotalEvents = interleavedEvents.Count;
            this.CompleteOrderCount = orders.Count(x => x.IsComplete);
            this.ConfirmedCount = orders.Count(x => x.IsConfirmed);
            this.AbandonedCount = orders.Count(x => x.IsAbandoned);
            this.RefundCount = orders.Count(x => x.HasRefund);
            this.EventTypeCounts = interleavedEvents
                                   .GroupBy(x => x.EventTypeName, StringComparer.Ordinal)
                                   .ToDictionary(g => g.Key, g => g.Count(), StringComparer.Ordinal);
        }

        public IReadOnlyList<OrderPlan> Orders { get; }

        public IReadOnlyList<InterleavedEvent> InterleavedEvents { get; }

        public int TotalOrders { get; }

        public int TotalEvents { get; }

        public int CompleteOrderCount { get; }

        public int ConfirmedCount { get; }

        public int AbandonedCount { get; }

        public int RefundCount { get; }

        public IReadOnlyDictionary<string, int> EventTypeCounts { get; }

        public static GenerationPlan Create(IReadOnlyList<OrderPlan> orders, IReadOnlyList<InterleavedEvent> interleavedEvents)
        {
            return new GenerationPlan(orders, interleavedEvents);
        }
    }

    internal sealed class GeneratorOptions
    {
        public bool CreateSnapshot { get; init; }

        public required string? ConnectionString { get; init; }

        public int TargetEvents { get; init; } = 100_000;

        public int MinimumCompleteOrders { get; init; } = 10_000;

        public string StreamPrefix { get; init; } = "order-";

        public double AbandonRatio { get; init; } = 0.20d;

        public double RefundRatio { get; init; } = 0.10d;

        public int? Seed { get; init; }

        public string ReportPath { get; init; } = "test-data/kurrent-seed/generation-report.json";

        public string SnapshotDirectory { get; init; } = "test-data/kurrent-seed/seed-data";

        public string SnapshotManifestPath { get; init; } = "test-data/kurrent-seed/snapshot-manifest.json";

        public string KurrentImage { get; init; } = "kurrentplatform/kurrentdb:25.1";

        public string ContainerDataPath { get; init; } = "/var/lib/kurrentdb";

        public static GeneratorOptions Parse(string[] args)
        {
            var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];

                if (!arg.StartsWith("--", StringComparison.Ordinal))
                {
                    continue;
                }

                var key = arg[2..];

                if (i + 1 < args.Length
                    && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    map[key] = args[++i];
                }
                else
                {
                    map[key] = "true";
                }
            }

            return new GeneratorOptions
            {
                CreateSnapshot = ParseBool(map, "create-snapshot", false),
                ConnectionString = map.GetValueOrDefault("connection-string"),
                TargetEvents = ParseInt(map, "target-events", 100_000),
                MinimumCompleteOrders = ParseInt(map, "min-complete-orders", 10_000),
                StreamPrefix = map.GetValueOrDefault("stream-prefix") ?? "order-",
                AbandonRatio = ParseDouble(map, "abandon-ratio", 0.20d),
                RefundRatio = ParseDouble(map, "refund-ratio", 0.10d),
                Seed = map.TryGetValue("seed", out var seedValue) ? int.Parse(seedValue, CultureInfo.InvariantCulture) : null,
                ReportPath = map.GetValueOrDefault("report-path") ?? "test-data/kurrent-seed/generation-report.json",
                SnapshotDirectory = map.GetValueOrDefault("snapshot-dir") ?? "test-data/kurrent-seed/seed-data",
                SnapshotManifestPath = map.GetValueOrDefault("snapshot-manifest-path") ?? "test-data/kurrent-seed/snapshot-manifest.json",
                KurrentImage = map.GetValueOrDefault("kurrent-image") ?? "kurrentplatform/kurrentdb:25.1",
                ContainerDataPath = map.GetValueOrDefault("container-data-path") ?? "/var/lib/kurrentdb",
            };
        }

        private static int ParseInt(IReadOnlyDictionary<string, string> args, string key, int fallback)
        {
            return args.TryGetValue(key, out var raw) ? int.Parse(raw, CultureInfo.InvariantCulture) : fallback;
        }

        private static double ParseDouble(IReadOnlyDictionary<string, string> args, string key, double fallback)
        {
            return args.TryGetValue(key, out var raw) ? double.Parse(raw, CultureInfo.InvariantCulture) : fallback;
        }

        private static bool ParseBool(IReadOnlyDictionary<string, string> args, string key, bool fallback)
        {
            if (!args.TryGetValue(key, out var raw))
            {
                return fallback;
            }

            if (string.Equals(raw, "true", StringComparison.OrdinalIgnoreCase)
                || string.Equals(raw, "1", StringComparison.Ordinal))
            {
                return true;
            }

            if (string.Equals(raw, "false", StringComparison.OrdinalIgnoreCase)
                || string.Equals(raw, "0", StringComparison.Ordinal))
            {
                return false;
            }

            return fallback;
        }
    }

    internal sealed class GenerationReport
    {
        public required string GeneratedAtUtc { get; init; }

        public required string CompletedAtUtc { get; init; }

        public required int TotalEvents { get; init; }

        public required int TotalOrders { get; init; }

        public required int CompleteOrderCount { get; init; }

        public required int ConfirmedCount { get; init; }

        public required int AbandonedCount { get; init; }

        public required int RefundCount { get; init; }

        public required string StreamPrefix { get; init; }

        public required double AbandonRatio { get; init; }

        public required double RefundRatio { get; init; }

        public required Dictionary<string, int> EventTypeCounts { get; init; }

        public int? Seed { get; init; }

        public static GenerationReport FromPlan(
            GenerationPlan plan,
            GeneratorOptions options,
            DateTimeOffset startedAt,
            DateTimeOffset completedAt)
        {
            return new GenerationReport
            {
                GeneratedAtUtc = startedAt.ToString("O", CultureInfo.InvariantCulture),
                CompletedAtUtc = completedAt.ToString("O", CultureInfo.InvariantCulture),
                TotalEvents = plan.TotalEvents,
                TotalOrders = plan.TotalOrders,
                CompleteOrderCount = plan.CompleteOrderCount,
                ConfirmedCount = plan.ConfirmedCount,
                AbandonedCount = plan.AbandonedCount,
                RefundCount = plan.RefundCount,
                StreamPrefix = options.StreamPrefix,
                AbandonRatio = options.AbandonRatio,
                RefundRatio = options.RefundRatio,
                Seed = options.Seed,
                EventTypeCounts = new Dictionary<string, int>(plan.EventTypeCounts, StringComparer.Ordinal),
            };
        }

        public async Task WriteAsync(string path)
        {
            var directory = Path.GetDirectoryName(path);

            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            await using var file = File.Create(path);
            await JsonSerializer.SerializeAsync(
                file,
                this,
                new JsonSerializerOptions(JsonSerializerDefaults.Web)
                {
                    WriteIndented = true,
                });

            await file.WriteAsync(Encoding.UTF8.GetBytes(Environment.NewLine));
        }
    }
}
