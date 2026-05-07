namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed class OrderStreamGeneratorOptions
{
    public int TargetEvents { get; init; } = 100_000;

    public int MinimumCompleteOrders { get; init; } = 10_000;

    public string StreamPrefix { get; init; } = "order-";

    public double AbandonRatio { get; init; } = 0.20d;

    public double RefundRatio { get; init; } = 0.10d;

    public int? Seed { get; init; }
}
