namespace Kuna.Examples.EventsSeeder.Seeding;

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
