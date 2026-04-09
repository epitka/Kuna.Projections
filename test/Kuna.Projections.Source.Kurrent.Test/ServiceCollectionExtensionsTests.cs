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
    public void AddEventStoreSource_Should_Throw_When_EventStore_ConnectionString_Is_Missing()
    {
        var services = new ServiceCollection();
        var configuration = BuildConfiguration(new Dictionary<string, string?>());

        services.AddEventStoreSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }));

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<KurrentDB.Client.KurrentDBClient>());
        ex.Message.ShouldContain("EventStore");
    }

    [Fact]
    public void AddEventStoreSource_Should_Throw_When_StreamName_Is_Missing()
    {
        var services = new ServiceCollection();
        var values = CreateValidSettings();
        values["EventStoreSource:EventsBoundedCapacity"] = "4096";
        var configuration = BuildConfiguration(values);

        services.AddEventStoreSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }));

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<EventStoreSourceSettings>());
        ex.Message.ShouldContain("StreamName");
    }

    [Fact]
    public void AddEventStoreSource_Should_Register_Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var values = CreateValidSettings();
        values["EventStoreSource:StreamName"] = "orders";

        var configuration = BuildConfiguration(values);

        services.AddEventStoreSource<TestModel>(
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

        var sourceSettings = provider.GetRequiredService<EventStoreSourceSettings>();
        sourceSettings.StreamName.ShouldBe("orders");
    }

    [Fact]
    public void AddEventStoreSource_Should_Bind_Settings_From_Custom_Section_When_Provided()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["ConnectionStrings:EventStore"] = "esdb://localhost:2113?tls=false",
                ["OrdersProjection:EventStoreSource:StreamName"] = "order-",
                ["OrdersProjection:EventStoreSource:EventsBoundedCapacity"] = "4096",
                ["OrdersProjection:EventStoreSource:ModelIdResolutionStrategy"] = "RequireStreamId",
            });

        services.AddEventStoreSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            "OrdersProjection:EventStoreSource");

        using var provider = services.BuildServiceProvider();

        var sourceSettings = provider.GetRequiredService<EventStoreSourceSettings>();
        sourceSettings.StreamName.ShouldBe("order-");
        sourceSettings.EventsBoundedCapacity.ShouldBe(4096);
        sourceSettings.ModelIdResolutionStrategy.ShouldBe(ModelIdResolutionStrategy.RequireStreamId);
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
            ["ConnectionStrings:EventStore"] = "esdb://localhost:2113?tls=false",
        };
    }

    private sealed class TestModel : Model
    {
    }
}
