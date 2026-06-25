using System.Runtime.InteropServices;
using DotNet.Testcontainers.Configurations;
using EventSourcingDb;
using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

/// <summary>
/// Starts a disposable EventSourcingDB container using the test helper shipped with
/// the .NET SDK and exposes a connected client to the integration tests.
/// </summary>
public class EventSourcingDbContainerFixture : IAsyncLifetime
{
    private readonly Container container;

    public EventSourcingDbContainerFixture()
    {
        ConfigureDockerHostForMac();
        this.container = new Container();
    }

    public IClient Client { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await this.container.StartAsync();
        this.Client = this.container.GetClient();
    }

    public async ValueTask DisposeAsync()
    {
        await this.container.StopAsync();
    }

    private static void ConfigureDockerHostForMac()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return;
        }

        if (!string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DOCKER_HOST")))
        {
            DisableResourceReaperForColima(Environment.GetEnvironmentVariable("DOCKER_HOST"));
            return;
        }

        var userName = Environment.GetEnvironmentVariable("USER") ?? Environment.UserName;
        var colimaSocketPath = $"/Users/{userName}/.colima/default/docker.sock";

        if (File.Exists(colimaSocketPath))
        {
            TestcontainersSettings.ResourceReaperEnabled = false;
            Environment.SetEnvironmentVariable("DOCKER_HOST", $"unix://{colimaSocketPath}");
        }
    }

    private static void DisableResourceReaperForColima(string? dockerHost)
    {
        if (dockerHost?.Contains("colima", StringComparison.OrdinalIgnoreCase) == true)
        {
            TestcontainersSettings.ResourceReaperEnabled = false;
        }
    }
}
