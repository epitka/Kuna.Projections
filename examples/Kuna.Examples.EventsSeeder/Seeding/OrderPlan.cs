namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record OrderPlan(
    string StreamName,
    Guid OrderId,
    bool IsComplete,
    bool IsConfirmed,
    bool IsAbandoned,
    bool HasRefund,
    IReadOnlyList<OrderEventEnvelope> Events);
