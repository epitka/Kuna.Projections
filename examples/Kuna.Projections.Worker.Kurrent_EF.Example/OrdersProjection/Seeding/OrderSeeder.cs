using Kuna.StreamGenerator;

namespace Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection.Seeding;

public sealed record OrderSeedRequest(
    string ConnectionString,
    int TargetEvents = 100_000,
    int MinimumCompleteOrders = 10_000,
    string StreamPrefix = "order-",
    double AbandonRatio = 0.20d,
    double RefundRatio = 0.10d,
    int? Seed = null,
    string? ReportPath = null);

public sealed record OrderSeedResult(
    int TotalEvents,
    int TotalOrders,
    int CompleteOrderCount,
    int ConfirmedCount,
    int AbandonedCount,
    int RefundCount,
    IReadOnlyDictionary<string, int> EventTypeCounts,
    string ReportPath);

public static class OrderSeeder
{
    public static async Task<OrderSeedResult> RunAsync(
        OrderSeedRequest request,
        CancellationToken cancellationToken = default)
    {
        var generator = new OrderStreamGenerator(
            new OrderStreamGeneratorOptions
            {
                TargetEvents = request.TargetEvents,
                MinimumCompleteOrders = request.MinimumCompleteOrders,
                StreamPrefix = request.StreamPrefix,
                AbandonRatio = request.AbandonRatio,
                RefundRatio = request.RefundRatio,
                Seed = request.Seed,
            });

        var plan = generator.BuildPlan();
        var writePlan = OrderStreamWritePlanFactory.Create(plan);
        await KurrentStreamWriter.WriteAsync(writePlan, request.ConnectionString, cancellationToken: cancellationToken);

        var reportPath = request.ReportPath ?? "test-data/kurrent-seed/generation-report.json";
        var report = OrderGenerationReportFactory.Create(
            plan,
            request.StreamPrefix,
            request.AbandonRatio,
            request.RefundRatio,
            request.Seed,
            DateTimeOffset.UtcNow,
            DateTimeOffset.UtcNow);

        await OrderGenerationReportWriter.WriteAsync(report, reportPath);

        return new OrderSeedResult(
            plan.TotalEvents,
            plan.TotalOrders,
            plan.CompleteOrderCount,
            plan.ConfirmedCount,
            plan.AbandonedCount,
            plan.RefundCount,
            new Dictionary<string, int>(plan.EventTypeCounts, StringComparer.Ordinal),
            reportPath);
    }
}
