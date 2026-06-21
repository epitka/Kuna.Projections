using EventSourcingDb;
using FakeItEasy;
using Kuna.Projections.Source.EventSourcingDB;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

public class EventSourcingDbHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_Should_Return_Healthy_When_Ping_Succeeds()
    {
        var client = A.Fake<IClient>();
        A.CallTo(() => client.PingAsync(A<CancellationToken>._)).Returns(Task.CompletedTask);
        var sut = new EventSourcingDbHealthCheck(client);

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task CheckHealthAsync_Should_Return_Unhealthy_When_Ping_Throws()
    {
        var client = A.Fake<IClient>();
        A.CallTo(() => client.PingAsync(A<CancellationToken>._))
         .ThrowsAsync(new InvalidOperationException("unreachable"));

        var sut = new EventSourcingDbHealthCheck(client);

        var result = await sut.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description?.ShouldContain("Disconnected");
        result.Exception.ShouldNotBeNull();
    }
}
