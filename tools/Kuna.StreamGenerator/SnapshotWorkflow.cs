using System.Runtime.InteropServices;
using Testcontainers.KurrentDb;

namespace Kuna.StreamGenerator;

public sealed record SnapshotRequest(
    string SnapshotDirectory,
    string SnapshotManifestPath,
    string KurrentImage = "kurrentplatform/kurrentdb:25.1",
    string ContainerDataPath = "/var/lib/kurrentdb",
    int TargetEvents = 100_000,
    int MinimumCompleteOrders = 10_000,
    string StreamPrefix = "order-",
    double AbandonRatio = 0.20d,
    double RefundRatio = 0.10d,
    int? Seed = null);

public sealed record SnapshotResult(
    string SnapshotDirectory,
    string SnapshotManifestPath,
    string KurrentImage,
    string ContainerDataPath,
    SeedGeneratorResult SeedResult);

public static class SnapshotWorkflow
{
    public static async Task<SnapshotResult> CreateSnapshotAsync(
        SnapshotRequest request,
        CancellationToken cancellationToken = default)
    {
        var snapshotDirectory = Path.GetFullPath(request.SnapshotDirectory);
        Directory.CreateDirectory(snapshotDirectory);
        ClearDirectory(snapshotDirectory);

        var reportPath = Path.Combine(
            Path.GetDirectoryName(request.SnapshotManifestPath) ?? Path.GetDirectoryName(snapshotDirectory) ?? ".",
            "generation-report.json");

        var builder = new KurrentDbBuilder(request.KurrentImage)
                      .WithAutoRemove(true)
                      .WithCleanUp(true)
                      .WithBindMount(snapshotDirectory, request.ContainerDataPath);

        var dockerEndpoint = ResolveDockerEndpointForMacColima();

        if (dockerEndpoint != null)
        {
            builder = builder.WithDockerEndpoint(dockerEndpoint);
        }

        await using var container = builder.Build();
        await container.StartAsync(cancellationToken);

        try
        {
            var connectionString = ((KurrentDbContainer)container).GetConnectionString();
            var seedResult = await GeneratorFacade.GenerateAndWriteAsync(
                                 new SeedGeneratorRequest(
                                     ConnectionString: connectionString,
                                     TargetEvents: request.TargetEvents,
                                     MinimumCompleteOrders: request.MinimumCompleteOrders,
                                     StreamPrefix: request.StreamPrefix,
                                     AbandonRatio: request.AbandonRatio,
                                     RefundRatio: request.RefundRatio,
                                     Seed: request.Seed,
                                     ReportPath: reportPath),
                                 cancellationToken);

            var snapshotResult = new SnapshotResult(
                snapshotDirectory,
                Path.GetFullPath(request.SnapshotManifestPath),
                request.KurrentImage,
                request.ContainerDataPath,
                seedResult);

            await WriteSnapshotManifestAsync(snapshotResult, cancellationToken);
            return snapshotResult;
        }
        finally
        {
            await container.StopAsync(cancellationToken);
        }
    }

    private static void ClearDirectory(string path)
    {
        foreach (var entry in Directory.EnumerateFileSystemEntries(path))
        {
            var fileName = Path.GetFileName(entry);

            if (string.Equals(fileName, ".gitkeep", StringComparison.Ordinal))
            {
                continue;
            }

            if (Directory.Exists(entry))
            {
                Directory.Delete(entry, recursive: true);
            }
            else
            {
                File.Delete(entry);
            }
        }
    }

    private static async Task WriteSnapshotManifestAsync(SnapshotResult result, CancellationToken cancellationToken)
    {
        var manifest = new
        {
            generatedAtUtc = DateTimeOffset.UtcNow,
            kurrentImage = result.KurrentImage,
            containerDataPath = result.ContainerDataPath,
            snapshotDirectory = result.SnapshotDirectory,
            seed = new
            {
                totalEvents = result.SeedResult.TotalEvents,
                totalOrders = result.SeedResult.TotalOrders,
                completeOrderCount = result.SeedResult.CompleteOrderCount,
                confirmedCount = result.SeedResult.ConfirmedCount,
                abandonedCount = result.SeedResult.AbandonedCount,
                refundCount = result.SeedResult.RefundCount,
                eventTypeCounts = result.SeedResult.EventTypeCounts,
                reportPath = result.SeedResult.ReportPath,
            },
        };

        var manifestPath = Path.GetFullPath(result.SnapshotManifestPath);
        var manifestDir = Path.GetDirectoryName(manifestPath);

        if (!string.IsNullOrWhiteSpace(manifestDir))
        {
            Directory.CreateDirectory(manifestDir);
        }

        await using var stream = File.Create(manifestPath);
        await System.Text.Json.JsonSerializer.SerializeAsync(
            stream,
            manifest,
            new System.Text.Json.JsonSerializerOptions(System.Text.Json.JsonSerializerDefaults.Web)
            {
                WriteIndented = true,
            },
            cancellationToken);
    }

    private static Uri? ResolveDockerEndpointForMacColima()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return null;
        }

        var dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");

        if (!string.IsNullOrEmpty(dockerHost)
            && dockerHost.StartsWith("unix://", StringComparison.OrdinalIgnoreCase)
            && dockerHost.Contains("colima", StringComparison.OrdinalIgnoreCase))
        {
            return new Uri(dockerHost);
        }

        var userName = Environment.GetEnvironmentVariable("USER") ?? Environment.UserName;
        var colimaSocketPath = $"/Users/{userName}/.colima/default/docker.sock";

        if (!File.Exists(colimaSocketPath))
        {
            return null;
        }

        var endpoint = $"unix://{colimaSocketPath}";
        Environment.SetEnvironmentVariable("DOCKER_HOST", endpoint);
        return new Uri(endpoint);
    }
}
