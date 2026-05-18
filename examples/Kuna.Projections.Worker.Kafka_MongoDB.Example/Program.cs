using System.Diagnostics.CodeAnalysis;
using Kuna.Projections.Core;
using Kuna.Projections.Worker.Kafka_MongoDB.Example.OrdersProjection;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Serilog;

Log.Logger = new LoggerConfiguration()
             .WriteTo.Console()
             .WriteTo.Debug()
             .CreateBootstrapLogger();

Log.Information("Starting up");

try
{
    var builder = WebApplication.CreateBuilder(args);

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
                       .AddCommandLine(args);
               });

    builder.Host
           .UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

    builder.Services.AddProjectionHost(typeof(Program).Assembly);
    builder.Services.AddOrdersProjections(builder.Configuration);

    var app = builder.Build();

    app.MapGet("/", () => "Kafka-backed MongoDB projection worker is running");
    app.MapHealthChecks("/health");
    app.MapGet(
        "/diagnostics/orders/status",
        async (OrdersKafkaStatusDiagnostics diagnostics, CancellationToken cancellationToken) =>
        {
            var result = await diagnostics.RunAsync(cancellationToken);
            return Results.Ok(result);
        });
    app.MapPost(
        "/diagnostics/orders/replay-consistency",
        async (
            ReplayConsistencyRequest? request,
            OrdersKafkaReplayConsistencyDiagnostics diagnostics,
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

namespace Kuna.Projections.Worker.Kafka_MongoDB.Example
{
    [ExcludeFromCodeCoverage]
    public partial class Program
    {
    }
}
