using Bogus;
using Kuna.Examples.EventsSeeder.Events;
using Kuna.Examples.EventsSeeder.Model;
using Kuna.Projections.Abstractions.Models;
using Kuna.StreamGenerator;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed class OrderStreamGenerator
{
    private static readonly string[] CurrencyCodes = ["USD", "EUR", "GBP", "CAD",];
    private static readonly string[] ProductTypes = ["physical", "digital", "subscription",];
    private readonly OrderStreamGeneratorOptions options;
    private readonly Random random;
    private readonly Faker faker;

    public OrderStreamGenerator(OrderStreamGeneratorOptions options)
    {
        this.options = options;
        this.random = options.Seed.HasValue ? new Random(options.Seed.Value) : Random.Shared;
        this.faker = new Faker();

        if (options.Seed.HasValue)
        {
            Randomizer.Seed = new Random(options.Seed.Value);
        }
    }

    public OrderGenerationPlan BuildPlan()
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

        return OrderGenerationPlan.Create(orders, interleaved);
    }

    private static void Validate(IReadOnlyList<InterleavedOrderEvent> interleaved, IReadOnlyList<OrderPlan> orders)
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

        var weighted = new List<int>(8);

        foreach (var candidate in candidates)
        {
            weighted.Add(candidate);

            if (candidate >= 3)
            {
                weighted.Add(candidate);
                weighted.Add(candidate);
            }

            if (candidate == 4)
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

        var events = new List<OrderEventEnvelope>(4);
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

        events.Add(new OrderEventEnvelope(streamName, 0, created));

        if (targetPhase >= 2)
        {
            var fee = decimal.Round(Math.Max(0.01m, amount * decimal.Round((decimal)this.faker.Random.Double(0.01, 0.08), 4)), 2);
            var feesCalculated = new OrderFeesCalculated
            {
                OrderId = orderId,
                FeeAmount = fee,
                CreatedOn = feeAt.UtcDateTime,
                TypeName = nameof(OrderFeesCalculated),
            };

            events.Add(new OrderEventEnvelope(streamName, 1, feesCalculated));
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
                                 : new OrderAbandoned
                                 {
                                     OrderId = orderId,
                                     CompletedDateTime = terminalAt,
                                     CreatedOn = terminalAt.UtcDateTime,
                                     TypeName = nameof(OrderAbandoned),
                                 };

            events.Add(new OrderEventEnvelope(streamName, 2, terminal));
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

            events.Add(new OrderEventEnvelope(streamName, 3, refund));
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
        var now = DateTimeOffset.UtcNow;
        var windowStart = now.AddDays(-90);
        var totalSeconds = (int)(now - windowStart).TotalSeconds;
        var offsetSeconds = this.random.Next(0, Math.Max(totalSeconds, 1));
        return windowStart.AddSeconds(offsetSeconds);
    }

    private List<InterleavedOrderEvent> Interleave(IReadOnlyList<OrderPlan> orders)
    {
        var states = orders
                     .Select(order => new OrderCursor(order))
                     .ToList();

        var active = Enumerable.Range(0, states.Count).ToList();
        var result = new List<InterleavedOrderEvent>(orders.Sum(o => o.Events.Count));
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
            result.Add(new InterleavedOrderEvent(cursor.Order.StreamName, next.Event, next.SequenceInStream));
            lastChosen = stateIndex;

            if (!cursor.HasNext)
            {
                active.RemoveAt(pickIndex);
            }
        }

        return result;
    }
}

public sealed record OrderPlan(
    string StreamName,
    Guid OrderId,
    bool IsComplete,
    bool IsConfirmed,
    bool IsAbandoned,
    bool HasRefund,
    IReadOnlyList<OrderEventEnvelope> Events);

public sealed record OrderEventEnvelope(string StreamName, int SequenceInStream, Event Event)
{
    public string EventTypeName => this.Event.TypeName;
}

public sealed record InterleavedOrderEvent(string StreamName, Event Event, int SequenceInStream)
{
    public string EventTypeName => this.Event.TypeName;
}

public sealed class OrderGenerationPlan
{
    private OrderGenerationPlan(
        IReadOnlyList<OrderPlan> orders,
        IReadOnlyList<InterleavedOrderEvent> interleavedEvents)
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

    public IReadOnlyList<InterleavedOrderEvent> InterleavedEvents { get; }

    public int TotalOrders { get; }

    public int TotalEvents { get; }

    public int CompleteOrderCount { get; }

    public int ConfirmedCount { get; }

    public int AbandonedCount { get; }

    public int RefundCount { get; }

    public IReadOnlyDictionary<string, int> EventTypeCounts { get; }

    public static OrderGenerationPlan Create(
        IReadOnlyList<OrderPlan> orders,
        IReadOnlyList<InterleavedOrderEvent> interleavedEvents)
    {
        return new OrderGenerationPlan(orders, interleavedEvents);
    }
}

public sealed class OrderStreamGeneratorOptions
{
    public int TargetEvents { get; init; } = 100_000;

    public int MinimumCompleteOrders { get; init; } = 10_000;

    public string StreamPrefix { get; init; } = "order-";

    public double AbandonRatio { get; init; } = 0.20d;

    public double RefundRatio { get; init; } = 0.10d;

    public int? Seed { get; init; }
}

public sealed class OrderGenerationReport
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
}

public static class OrderStreamWritePlanFactory
{
    public static StreamWritePlan Create(OrderGenerationPlan plan)
    {
        return new StreamWritePlan(
            plan.InterleavedEvents
                .Select(x => new StreamWriteEvent(x.StreamName, x.EventTypeName, x.Event))
                .ToArray());
    }
}

public static class OrderGenerationReportFactory
{
    public static OrderGenerationReport Create(
        OrderGenerationPlan plan,
        string streamPrefix,
        double abandonRatio,
        double refundRatio,
        int? seed,
        DateTimeOffset startedAt,
        DateTimeOffset completedAt)
    {
        return new OrderGenerationReport
        {
            GeneratedAtUtc = startedAt.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            CompletedAtUtc = completedAt.ToString("O", System.Globalization.CultureInfo.InvariantCulture),
            TotalEvents = plan.TotalEvents,
            TotalOrders = plan.TotalOrders,
            CompleteOrderCount = plan.CompleteOrderCount,
            ConfirmedCount = plan.ConfirmedCount,
            AbandonedCount = plan.AbandonedCount,
            RefundCount = plan.RefundCount,
            StreamPrefix = streamPrefix,
            AbandonRatio = abandonRatio,
            RefundRatio = refundRatio,
            Seed = seed,
            EventTypeCounts = new Dictionary<string, int>(plan.EventTypeCounts, StringComparer.Ordinal),
        };
    }
}

public static class OrderGenerationReportWriter
{
    public static async Task WriteAsync(OrderGenerationReport report, string path)
    {
        var directory = Path.GetDirectoryName(path);

        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await using var file = File.Create(path);
        await System.Text.Json.JsonSerializer.SerializeAsync(
            file,
            report,
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)
            {
                WriteIndented = true,
            });

        await file.WriteAsync(System.Text.Encoding.UTF8.GetBytes(Environment.NewLine));
    }
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

    public OrderEventEnvelope Next()
    {
        if (!this.HasNext)
        {
            throw new InvalidOperationException("No more events in cursor.");
        }

        return this.Order.Events[this.index++];
    }
}
