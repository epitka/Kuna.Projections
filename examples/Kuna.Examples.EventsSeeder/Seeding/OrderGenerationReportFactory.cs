namespace Kuna.Examples.EventsSeeder.Seeding;

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
