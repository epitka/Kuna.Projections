using System.Text;
using Bogus;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.Integration.Test.LocalOrders;
using KurrentDB.Client;
using Newtonsoft.Json;

namespace Kuna.Projections.Pipeline.Integration.Test;

internal static class GeneratedOrderEventSeeder
{
    private static readonly string[] CurrencyCodes = ["USD", "EUR", "GBP", "CAD",];
    private static readonly string[] ProductTypes = ["physical", "digital", "subscription",];

    private static readonly JsonSerializerSettings SerializerSettings = new()
    {
        TypeNameHandling = TypeNameHandling.None,
        Formatting = Formatting.None,
        NullValueHandling = NullValueHandling.Include,
    };

    public static SeedPlan BuildPlan(
        int targetEvents,
        int minimumCompleteOrders,
        string streamPrefix,
        double abandonRatio,
        double refundRatio,
        int? seed)
    {
        var planner = new Planner(targetEvents, minimumCompleteOrders, streamPrefix, abandonRatio, refundRatio, seed);
        var plan = planner.BuildPlan();
        var plannedEvents = plan.InterleavedEvents
                                .Select(x => new PlannedEvent(x.StreamName, x.Event))
                                .ToList();

        return new SeedPlan(plannedEvents, plan.Streams.Count);
    }

    public static async Task<SeedResult> SeedAsync(
        string connectionString,
        int targetEvents,
        int minimumCompleteOrders,
        string streamPrefix,
        double abandonRatio,
        double refundRatio,
        int? seed,
        CancellationToken cancellationToken)
    {
        var plan = BuildPlan(targetEvents, minimumCompleteOrders, streamPrefix, abandonRatio, refundRatio, seed);
        var appendedCount = await AppendAsync(connectionString, plan.Events, cancellationToken);
        return new SeedResult(appendedCount, plan.StreamCount);
    }

    public static async Task<int> AppendAsync(
        string connectionString,
        IReadOnlyList<PlannedEvent> plannedEvents,
        CancellationToken cancellationToken)
    {
        using var client = new KurrentDBClient(KurrentDBClientSettings.Create(connectionString));

        foreach (var item in plannedEvents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var payload = Serialize(item.Event);
            var eventData = new EventData(Uuid.NewUuid(), item.Event.GetType().Name, payload);
            await client.AppendToStreamAsync(item.StreamName, StreamState.Any, [eventData,], cancellationToken: cancellationToken);
        }

        return plannedEvents.Count;
    }

    private static byte[] Serialize(object obj)
    {
        return Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(obj, SerializerSettings));
    }

    private sealed class Planner
    {
        private readonly Random random;
        private readonly Faker faker;
        private readonly int targetEvents;
        private readonly int minimumCompleteOrders;
        private readonly string streamPrefix;
        private readonly double abandonRatio;
        private readonly double refundRatio;

        public Planner(
            int targetEvents,
            int minimumCompleteOrders,
            string streamPrefix,
            double abandonRatio,
            double refundRatio,
            int? seed)
        {
            this.targetEvents = targetEvents;
            this.minimumCompleteOrders = minimumCompleteOrders;
            this.streamPrefix = streamPrefix;
            this.abandonRatio = abandonRatio;
            this.refundRatio = refundRatio;
            this.random = seed.HasValue ? new Random(seed.Value) : Random.Shared;
            this.faker = new Faker();

            if (seed.HasValue)
            {
                Randomizer.Seed = new Random(seed.Value);
            }
        }

        public Plan BuildPlan()
        {
            var orders = new List<OrderPlan>(40000);
            var totalEvents = 0;
            var nextOrderNumber = 1;

            for (var i = 0; i < this.minimumCompleteOrders; i++)
            {
                var order = this.CreateOrder(nextOrderNumber++, forceComplete: true);
                orders.Add(order);
                totalEvents += order.Events.Count;
            }

            var remaining = this.targetEvents - totalEvents;

            while (remaining > 0)
            {
                var phase = this.ChoosePhaseForRemaining(remaining);
                var order = this.CreateOrder(nextOrderNumber++, forceComplete: false, fixedPhase: phase);
                orders.Add(order);
                remaining -= order.Events.Count;
            }

            var interleaved = this.Interleave(orders);
            return new Plan(interleaved, orders.Select(x => x.StreamName).ToHashSet(StringComparer.Ordinal));
        }

        private int ChoosePhaseForRemaining(int remaining)
        {
            if (remaining <= 4)
            {
                return remaining;
            }

            var weighted = new List<int>(16);
            var candidates = Enumerable.Range(1, 4).Where(p => p <= remaining);

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

        private OrderPlan CreateOrder(int orderSequence, bool forceComplete, int? fixedPhase = null)
        {
            var orderId = Guid.NewGuid();
            var streamName = $"{this.streamPrefix}{orderId:D}";
            var isAbandoned = this.random.NextDouble() < this.abandonRatio;
            var isConfirmed = !isAbandoned;
            var includeRefund = isConfirmed && this.random.NextDouble() < this.refundRatio;
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

            var events = new List<SeedEvent>(4)
            {
                new(
                    streamName,
                    new OrderCreatedEvent
                    {
                        Id = orderId,
                        OrderNumber = $"ORD-{orderSequence:D7}",
                        MerchantId = Guid.NewGuid(),
                        CreatedDateTime = createdAt,
                        PaymentAuthorizationId = $"payauth_{this.faker.Random.AlphaNumeric(16)}",
                        Amount = amount,
                        ShippingAmount = shipping,
                        TaxAmount = tax,
                        CurrencyCode = currencyCode,
                        CustomerId = this.faker.Random.Bool(0.8f) ? Guid.NewGuid() : null,
                        ProductType = this.faker.PickRandom(ProductTypes),
                        CreatedOn = createdAt.UtcDateTime,
                        TypeName = nameof(OrderCreatedEvent),
                    }),
            };

            if (targetPhase >= 2)
            {
                var fee = decimal.Round(Math.Max(0.01m, amount * decimal.Round((decimal)this.faker.Random.Double(0.01, 0.08), 4)), 2);
                events.Add(
                    new SeedEvent(
                        streamName,
                        new OrderMerchantFeesCalculatedEvent
                        {
                            OrderId = orderId,
                            MerchantTransactionFeeAmount = fee,
                            CreatedOn = feeAt.UtcDateTime,
                            TypeName = nameof(OrderMerchantFeesCalculatedEvent),
                        }));
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

                events.Add(new SeedEvent(streamName, terminal));
            }

            if (targetPhase >= 4)
            {
                events.Add(
                    new SeedEvent(
                        streamName,
                        new RefundAppliedToOrderEvent
                        {
                            OrderId = orderId,
                            Amount = 1.00m,
                            RefundId = Guid.NewGuid(),
                            MerchantReference = $"rfnd_{this.faker.Random.AlphaNumeric(12)}",
                            CreatedOn = refundAt.UtcDateTime,
                            TypeName = nameof(RefundAppliedToOrderEvent),
                        }));
            }

            return new OrderPlan(streamName, events);
        }

        private List<SeedEvent> Interleave(IReadOnlyList<OrderPlan> orders)
        {
            var cursors = orders.Select(x => new Cursor(x)).ToList();
            var active = Enumerable.Range(0, cursors.Count).ToList();
            var result = new List<SeedEvent>(orders.Sum(x => x.Events.Count));

            while (active.Count > 0)
            {
                var idx = this.random.Next(active.Count);
                var orderIndex = active[idx];
                var cursor = cursors[orderIndex];
                result.Add(cursor.Next());

                if (cursor.Done)
                {
                    active.RemoveAt(idx);
                }
            }

            return result;
        }

        private DateTimeOffset RandomCreatedAt()
        {
            var now = DateTimeOffset.UtcNow;
            var windowStart = now.AddDays(-90);
            var totalSeconds = (int)(now - windowStart).TotalSeconds;
            return windowStart.AddSeconds(this.random.Next(0, Math.Max(totalSeconds, 1)));
        }

        private sealed class Cursor
        {
            private readonly OrderPlan order;
            private int index;

            public Cursor(OrderPlan order)
            {
                this.order = order;
                this.index = 0;
            }

            public bool Done => this.index >= this.order.Events.Count;

            public SeedEvent Next()
            {
                var evt = this.order.Events[this.index];
                this.index++;
                return evt;
            }
        }

        public sealed record OrderPlan(string StreamName, IReadOnlyList<SeedEvent> Events);

        public sealed record SeedEvent(string StreamName, Event Event);

        public sealed record Plan(List<SeedEvent> InterleavedEvents, HashSet<string> Streams);
    }

    internal sealed record PlannedEvent(string StreamName, Event Event);

    internal sealed record SeedPlan(IReadOnlyList<PlannedEvent> Events, int StreamCount);

    internal readonly record struct SeedResult(int EventCount, int StreamCount);
}
