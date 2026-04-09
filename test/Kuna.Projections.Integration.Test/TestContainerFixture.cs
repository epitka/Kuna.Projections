using System.Diagnostics;
using System.Runtime.InteropServices;
using DotNet.Testcontainers.Configurations;

namespace Kuna.Projections.Pipeline.Integration.Test;

public abstract class TestContainerFixture
{
    protected TestContainerFixture()
    {
        TestcontainersSettings.ResourceReaperEnabled =
            !string.Equals(Environment.GetEnvironmentVariable("KUNA_TEST_DISABLE_RESOURCE_REAPER"), "1", StringComparison.Ordinal);

        this.Configure();
    }

    protected Uri? DockerEndpoint { get; private set; }

    protected async ValueTask RemoveContainerIfPresentAsync(string containerName)
    {
        var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = "docker",
                ArgumentList = { "rm", "-f", containerName, },
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            },
        };

        process.Start();
        var standardError = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        if (process.ExitCode == 0
            || standardError.Contains("No such container", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        throw new InvalidOperationException($"Failed to remove stale Docker container '{containerName}': {standardError}");
    }

    private void Configure()
    {
        if (!RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return;
        }

        var dockerHost = Environment.GetEnvironmentVariable("DOCKER_HOST");

        if (!string.IsNullOrEmpty(dockerHost)
            && dockerHost.StartsWith("unix://", StringComparison.OrdinalIgnoreCase)
            && dockerHost.Contains("colima", StringComparison.OrdinalIgnoreCase))
        {
            this.DockerEndpoint = new Uri(dockerHost);
            return;
        }

        var userName = Environment.GetEnvironmentVariable("USER") ?? Environment.UserName;
        var colimaSocketPath = $"/Users/{userName}/.colima/default/docker.sock";

        if (!File.Exists(colimaSocketPath))
        {
            return;
        }

        var endpoint = $"unix://{colimaSocketPath}";
        this.DockerEndpoint = new Uri(endpoint);
        Environment.SetEnvironmentVariable("DOCKER_HOST", endpoint);
    }
}
