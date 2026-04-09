using System.Diagnostics.CodeAnalysis;
using Kuna.Projections.Core;
using Kuna.Projections.Worker.Template.AccountProjection;
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
                       .AddKeyPerFile(directoryPath: "/keyvault", optional: true)
                       .AddEnvironmentVariables()
                       .AddEnvironmentVariables("EVENTSTORE_")
                       .AddCommandLine(args);
               });

    builder.Host.UseSerilog((ctx, lc) => lc.ReadFrom.Configuration(ctx.Configuration));

    builder.Services.AddProjectionHost(typeof(Program).Assembly);
    builder.Services.AddAccountProjection(builder.Configuration);

    var app = builder.Build();

    app.MapGet("/", () => "Projection worker is running");

    app.Run();
}
catch (Exception ex)
{
    Log.Fatal(ex, "Unhandled exception");
}
finally
{
    Log.Information("Shut down complete");
    Log.CloseAndFlush();
}

namespace Kuna.Projections.Worker.Template
{
    [ExcludeFromCodeCoverage]
    public partial class Program
    {
    }
}
