using Kuna.StreamGenerator;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record OrderSeedRequest(
    string? ConnectionString = null,
    string? KafkaBootstrapServers = null,
    string? KafkaTopic = null,
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
        if (string.IsNullOrWhiteSpace(request.ConnectionString)
            && string.IsNullOrWhiteSpace(request.KafkaBootstrapServers))
        {
            throw new InvalidOperationException("OrderSeeder requires a KurrentDB connection string and/or Kafka bootstrap servers.");
        }

        if (!string.IsNullOrWhiteSpace(request.KafkaBootstrapServers)
            && string.IsNullOrWhiteSpace(request.KafkaTopic))
        {
            throw new InvalidOperationException("OrderSeeder requires KafkaTopic when KafkaBootstrapServers is provided.");
        }

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

        if (!string.IsNullOrWhiteSpace(request.ConnectionString))
        {
            await KurrentStreamWriter.WriteAsync(writePlan, request.ConnectionString, cancellationToken: cancellationToken);
        }

        if (!string.IsNullOrWhiteSpace(request.KafkaBootstrapServers))
        {
            await KafkaStreamWriter.WriteAsync(
                new KafkaWritePlan(request.KafkaTopic!, plan.InterleavedEvents),
                request.KafkaBootstrapServers,
                cancellationToken: cancellationToken);
        }

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
