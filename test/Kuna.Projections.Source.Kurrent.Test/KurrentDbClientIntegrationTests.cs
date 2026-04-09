using System.Text;
using Kuna.Projections.Source.Kurrent;
using KurrentDB.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

[Collection(KurrentDbCollection.Name)]
public class KurrentDbClientIntegrationTests
{
    private readonly KurrentDBContainerFixture fixture;

    public KurrentDbClientIntegrationTests(KurrentDBContainerFixture fixture)
    {
        this.fixture = fixture;
    }

    [Fact]
    public async Task EventStoreHealthCheck_Should_Be_Healthy_Against_Running_Kurrent()
    {
        if (!ShouldRunContainerTests())
        {
            return;
        }

        var client = CreateClient();
        var healthCheck = new EventStoreHealthCheck(client);

        var result = await healthCheck.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task KurrentClient_Should_Append_And_Read_Stream_Roundtrip()
    {
        if (!ShouldRunContainerTests())
        {
            return;
        }

        var client = CreateClient();
        var streamName = $"roundtrip-{Guid.NewGuid():N}";
        var payload = "{\"name\":\"demo\"}";
        var eventData = new EventData(
            Uuid.NewUuid(),
            "demo-created",
            Encoding.UTF8.GetBytes(payload));

        await client.AppendToStreamAsync(
            streamName,
            StreamState.NoStream,
            new[] { eventData, },
            cancellationToken: CancellationToken.None);

        var results = new List<ResolvedEvent>();

        await foreach (var resolved in client.ReadStreamAsync(
                           Direction.Forwards,
                           streamName,
                           StreamPosition.Start,
                           maxCount: 10,
                           cancellationToken: CancellationToken.None))
        {
            results.Add(resolved);
        }

        results.Count.ShouldBe(1);
        results[0].Event.EventType.ShouldBe("demo-created");
        Encoding.UTF8.GetString(results[0].Event.Data.ToArray()).ShouldBe(payload);
    }

    private static bool ShouldRunContainerTests()
    {
        return string.Equals(
            Environment.GetEnvironmentVariable("RUN_KURRENT_CONTAINER_TESTS"),
            "1",
            StringComparison.Ordinal);
    }

    private KurrentDBClient CreateClient()
    {
        var settings = KurrentDBClientSettings.Create(this.fixture.ConnectionString);
        return new KurrentDBClient(settings);
    }
}
