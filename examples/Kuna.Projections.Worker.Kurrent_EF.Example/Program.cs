using System.Diagnostics.CodeAnalysis;
using Kuna.Projections.Core;
using Kuna.Projections.Worker.Kurrent_EF.Example.OrdersProjection;
using Serilog;

Log.Logger = new LoggerConfiguration()
             .WriteTo.Console()
             .WriteTo.Debug()
             .CreateBootstrapLogger();

Log.Information("Starting up");

try
{
    var builder = WebApplication.CreateBuilder(args);

    // Add services to the container.
    builder.WebHost
           .ConfigureKestrel(
               options =>
               {
                   options.AddServerHeader = false;
               })
           .ConfigureAppConfiguration(
               (hostingContext, config) =>
               {
                   var environment = hostingContext.HostingEnvironment;

                   config
                       .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                       .AddJsonFile($"appsettings.{environment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                       .AddKeyPerFile(directoryPath: "/keyvault", optional: true)
                       .AddEnvironmentVariables()
                       .AddEnvironmentVariables("EVENTSTORE_")
                       .AddCommandLine(args);
               });

    builder.Host
           .UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

    builder.Services.AddProjectionHost(typeof(Program).Assembly);
    builder.Services.AddOrdersProjections(builder.Configuration);

    var app = builder.Build();

    app.MapGet("/", () => "Projection worker is running");
    app.MapPost(
        "/diagnostics/orders/replay-consistency",
        async (
            ReplayConsistencyRequest? request,
            OrdersReplayConsistencyDiagnostics diagnostics,
            CancellationToken cancellationToken) =>
        {
            var result = await diagnostics.RunAsync(request ?? new ReplayConsistencyRequest(), cancellationToken);
            return Results.Ok(result);
        });

    await app.RunAsync();
    return 0;
}
catch (Exception ex)
{
    Log.Fatal(ex, "Unhandled exception");
    return 1;
}
finally
{
    Log.Information("Shut down complete");
    Log.CloseAndFlush();
}

// Enables the class to be visible to integration tests by Microsoft.AspNetCore.Mvc.Testing
namespace Kuna.Projections.Worker.Kurrent_EF.Example
{
    [ExcludeFromCodeCoverage]
    public partial class Program
    {
    }
}
