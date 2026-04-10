using Kuna.Projections.Source.Kurrent;
using KurrentDB.Client;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class KurrentDbHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_Should_Return_Unhealthy_When_Kurrent_Is_Unreachable()
    {
        var settings = KurrentDBClientSettings.Create("esdb://localhost:1?tls=false");
        var client = new KurrentDBClient(settings);
        var sut = new KurrentDbHealthCheck(client);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), cts.Token);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description?.ShouldContain("Disconnected");
        result.Exception.ShouldNotBeNull();
    }
}
