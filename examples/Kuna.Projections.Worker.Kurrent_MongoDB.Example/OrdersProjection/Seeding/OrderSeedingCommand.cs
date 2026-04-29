using System.Globalization;
using Kuna.StreamGenerator;

namespace Kuna.Projections.Worker.Kurrent_MongoDB.Example.OrdersProjection.Seeding;

public static class OrderSeedingCommand
{
    public static async Task<int> RunAsync(string[] args, CancellationToken cancellationToken)
    {
        var options = SeederOptions.Parse(args);

        if (options.CreateSnapshot)
        {
            var snapshotStartedAt = DateTimeOffset.UtcNow;
            var snapshot = await SnapshotWorkflow.CreateSnapshotAsync(
                               new SnapshotRequest(
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
                               cancellationToken);

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
        var generator = new OrderStreamGenerator(
            new OrderStreamGeneratorOptions
            {
                TargetEvents = options.TargetEvents,
                MinimumCompleteOrders = options.MinimumCompleteOrders,
                StreamPrefix = options.StreamPrefix,
                AbandonRatio = options.AbandonRatio,
                RefundRatio = options.RefundRatio,
                Seed = options.Seed,
            });

        var orderPlan = generator.BuildPlan();
        var writePlan = OrderStreamWritePlanFactory.Create(orderPlan);

        Console.WriteLine($"Planned {orderPlan.TotalEvents} events across {orderPlan.TotalOrders} streams.");
        Console.WriteLine(
            $"Complete streams: {orderPlan.CompleteOrderCount}, confirmed: {orderPlan.ConfirmedCount}, abandoned: {orderPlan.AbandonedCount}, refunds: {orderPlan.RefundCount}.");

        var progress = new Progress<StreamWriteProgress>(
            x =>
            {
                if (x.WrittenEvents % 5000 == 0
                    || x.WrittenEvents == x.TotalEvents)
                {
                    Console.WriteLine(
                        $"Written {x.WrittenEvents}/{x.TotalEvents} events ({Math.Round(x.WrittenEvents / Math.Max(x.Elapsed.TotalSeconds, 1), 1)} ev/s)");
                }
            });

        await KurrentStreamWriter.WriteAsync(writePlan, options.ConnectionString, progress, cancellationToken);

        var completedAt = DateTimeOffset.UtcNow;
        var report = OrderGenerationReportFactory.Create(
            orderPlan,
            options.StreamPrefix,
            options.AbandonRatio,
            options.RefundRatio,
            options.Seed,
            startedAt,
            completedAt);

        await OrderGenerationReportWriter.WriteAsync(report, options.ReportPath);

        Console.WriteLine($"Report written to {options.ReportPath}");
        return 0;
    }

    public static bool IsSeedMode(string[] args)
    {
        return args.Any(
            arg => string.Equals(arg, "--seed", StringComparison.OrdinalIgnoreCase)
                   || string.Equals(arg, "--create-snapshot", StringComparison.OrdinalIgnoreCase));
    }

    internal sealed class SeederOptions
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

        public static SeederOptions Parse(string[] args)
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

            return new SeederOptions
            {
                CreateSnapshot = ParseBool(map, "create-snapshot", false),
                ConnectionString = map.GetValueOrDefault("connection-string"),
                TargetEvents = ParseInt(map, "target-events", 100_000),
                MinimumCompleteOrders = ParseInt(map, "min-complete-orders", 10_000),
                StreamPrefix = map.GetValueOrDefault("stream-prefix") ?? "order-",
                AbandonRatio = ParseDouble(map, "abandon-ratio", 0.20d),
                RefundRatio = ParseDouble(map, "refund-ratio", 0.10d),
                Seed = map.TryGetValue("seed", out var seedValue) && !string.Equals(seedValue, "true", StringComparison.OrdinalIgnoreCase)
                           ? int.Parse(seedValue, CultureInfo.InvariantCulture)
                           : null,
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
}
