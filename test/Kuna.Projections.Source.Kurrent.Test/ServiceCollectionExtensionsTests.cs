using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kurrent;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.Kurrent.Test;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddKurrentDBSource_Should_Throw_When_EventStore_ConnectionString_Is_Missing()
    {
        var services = new ServiceCollection();
        var values = CreateValidSettings();
        values.Remove("ConnectionStrings:KurrentDB");
        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }));

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<KurrentDB.Client.KurrentDBClient>());
        ex.Message.ShouldContain("KurrentDB");
    }

    [Fact]
    public void AddKurrentDBSource_Should_Throw_When_KurrentDb_Section_Is_Missing()
    {
        var services = new ServiceCollection();
        var values = CreateValidSettings();
        values.Remove("Projections:KurrentDB:Filter:Kind");
        values.Remove("Projections:KurrentDB:Filter:Prefixes:0");
        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }));

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<KurrentDbSourceSettings>());
        ex.Message.ShouldContain("Projections:KurrentDB");
    }

    [Fact]
    public void AddKurrentDBSource_Should_Register_Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var values = CreateValidSettings();
        values["Projections:KurrentDB:Filter:Prefixes:0"] = "orders-";

        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }));

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IEventDeserializer>().ShouldNotBeNull();
        provider.GetRequiredService<IEventModelIdResolver>().ShouldNotBeNull();
        provider.GetRequiredService<IEventEnvelopeFactory>().ShouldNotBeNull();
        provider.GetRequiredService<IEventSource<EventEnvelope>>().ShouldNotBeNull();

        var sourceSettings = provider.GetRequiredService<KurrentDbSourceSettings>();
        sourceSettings.Filter.Kind.ShouldBe(KurrentDbFilterKind.StreamPrefix);
        sourceSettings.Filter.Prefixes.Single().ShouldBe("orders-");
    }

    [Fact]
    public void AddKurrentDBSource_Should_Bind_Settings_From_Custom_Section_When_Provided()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["ConnectionStrings:KurrentDB"] = "esdb://localhost:2113?tls=false",
                ["OrdersProjection:Source"] = "KurrentDB",
                ["OrdersProjection:ModelIdResolutionStrategy"] = "RequireStreamId",
                ["OrdersProjection:KurrentDB:Filter:Kind"] = "StreamPrefix",
                ["OrdersProjection:KurrentDB:Filter:Prefixes:0"] = "order-",
            });

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            "OrdersProjection");

        using var provider = services.BuildServiceProvider();

        var sourceSettings = provider.GetRequiredService<KurrentDbSourceSettings>();
        sourceSettings.Filter.Kind.ShouldBe(KurrentDbFilterKind.StreamPrefix);
        sourceSettings.Filter.Prefixes.Single().ShouldBe("order-");
        provider.GetRequiredService<IProjectionSettings<TestModel>>().ModelIdResolutionStrategy.ShouldBe(ModelIdResolutionStrategy.RequireStreamId);
    }

    private static IConfiguration BuildConfiguration(IDictionary<string, string?> values)
    {
        return new ConfigurationBuilder()
               .AddInMemoryCollection(values)
               .Build();
    }

    private static Dictionary<string, string?> CreateValidSettings()
    {
        return new Dictionary<string, string?>
        {
            ["ConnectionStrings:KurrentDB"] = "esdb://localhost:2113?tls=false",
            ["Projections:Source"] = "KurrentDB",
            ["Projections:ModelIdResolutionStrategy"] = "PreferAttribute",
            ["Projections:KurrentDB:Filter:Kind"] = "StreamPrefix",
            ["Projections:KurrentDB:Filter:Prefixes:0"] = "orders-",
        };
    }

    private sealed class TestModel : Model
    {
    }
}
