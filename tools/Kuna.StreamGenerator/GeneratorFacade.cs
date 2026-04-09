using KurrentDB.Client;

namespace Kuna.StreamGenerator;

public sealed record SeedGeneratorRequest(
    string ConnectionString,
    int TargetEvents = 100_000,
    int MinimumCompleteOrders = 10_000,
    string StreamPrefix = "order-",
    double AbandonRatio = 0.20d,
    double RefundRatio = 0.10d,
    int? Seed = null,
    string? ReportPath = null);

public sealed record SeedGeneratorResult(
    int TotalEvents,
    int TotalOrders,
    int CompleteOrderCount,
    int ConfirmedCount,
    int AbandonedCount,
    int RefundCount,
    IReadOnlyDictionary<string, int> EventTypeCounts,
    string ReportPath);

public static class GeneratorFacade
{
    public static async Task<SeedGeneratorResult> GenerateAndWriteAsync(
        SeedGeneratorRequest request,
        CancellationToken cancellationToken = default)
    {
        var options = new global::Kuna.StreamGenerator.GeneratorOptions
        {
            ConnectionString = request.ConnectionString,
            TargetEvents = request.TargetEvents,
            MinimumCompleteOrders = request.MinimumCompleteOrders,
            StreamPrefix = request.StreamPrefix,
            AbandonRatio = request.AbandonRatio,
            RefundRatio = request.RefundRatio,
            Seed = request.Seed,
            ReportPath = request.ReportPath ?? "test-data/kurrent-seed/generation-report.json",
        };

        var generator = new global::Kuna.StreamGenerator.StreamGenerator(options);
        var plan = generator.BuildPlan();

        await WriteToKurrentAsync(plan, options, cancellationToken);

        var report = global::Kuna.StreamGenerator.GenerationReport.FromPlan(plan, options, DateTimeOffset.UtcNow, DateTimeOffset.UtcNow);
        await report.WriteAsync(options.ReportPath);

        return new SeedGeneratorResult(
            plan.TotalEvents,
            plan.TotalOrders,
            plan.CompleteOrderCount,
            plan.ConfirmedCount,
            plan.AbandonedCount,
            plan.RefundCount,
            new Dictionary<string, int>(plan.EventTypeCounts, StringComparer.Ordinal),
            options.ReportPath);
    }

    private static async Task WriteToKurrentAsync(
        global::Kuna.StreamGenerator.GenerationPlan plan,
        global::Kuna.StreamGenerator.GeneratorOptions options,
        CancellationToken cancellationToken)
    {
        var client = new KurrentDBClient(KurrentDBClientSettings.Create(options.ConnectionString!));
        var seenStreams = new HashSet<string>(StringComparer.Ordinal);

        foreach (var item in plan.InterleavedEvents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var payload = EventStoreJson.Serialize(item.Event);
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
                cancellationToken: cancellationToken);
        }
    }
}
