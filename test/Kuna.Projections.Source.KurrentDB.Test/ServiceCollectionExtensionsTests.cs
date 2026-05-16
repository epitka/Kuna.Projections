using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Examples.Events;
using Kuna.Projections.Source.KurrentDB;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.KurrentDB.Test;

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
                }),
            ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(() => provider.GetRequiredService<global::KurrentDB.Client.KurrentDBClient>());
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
                }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain("Projections:KurrentDB");
    }

    [Fact]
    public void AddKurrentDBSource_Should_Register_Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var values = CreateValidSettings();
        values["Projections:KurrentDB:Filter:Prefixes:1"] = "payments-";

        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredService<IEventDeserializer>().ShouldNotBeNull();
        provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IProjectionSettings<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name))
                .Source.ShouldBe(ProjectionSourceKind.KurrentDB);
    }

    [Fact]
    public void AddKurrentDBSource_Should_Register_Services_For_Stream_Regex_Filter()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var values = CreateValidSettings();
        values["Projections:KurrentDB:Filter:Kind"] = "StreamRegex";
        values.Remove("Projections:KurrentDB:Filter:Prefixes:0");
        values["Projections:KurrentDB:Filter:Regex"] = "^order|^invoice";

        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)).ShouldNotBeNull();
    }

    [Fact]
    public void AddKurrentDBSource_Should_Throw_When_Regex_Filter_Has_No_Regex()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var values = CreateValidSettings();
        values["Projections:KurrentDB:Filter:Kind"] = "StreamRegex";
        values.Remove("Projections:KurrentDB:Filter:Prefixes:0");
        values.Remove("Projections:KurrentDB:Filter:Regex");
        var configuration = BuildConfiguration(values);

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain("regular expression");
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
                ["OrdersProjection:KurrentDB:Filter:Kind"] = "EventTypePrefix",
                ["OrdersProjection:KurrentDB:Filter:Prefixes:0"] = "Order",
            });

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            "OrdersProjection");

        RegisterProjectionSettings<TestModel>(services, configuration, "OrdersProjection");

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>("OrdersProjection")).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IProjectionSettings<TestModel>>(GetRegistrationKey<TestModel>("OrdersProjection"))
                .ModelIdResolutionStrategy.ShouldBe(ModelIdResolutionStrategy.RequireStreamId);
    }

    [Fact]
    public void AddKurrentDBSource_Should_Discover_Event_Types_From_Referenced_Assemblies()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(CreateValidSettings());

        services.AddKurrentDBSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var eventDeserializer = provider.GetRequiredService<IEventDeserializer>();
        var payload = System.Text.Encoding.UTF8.GetBytes(
            """
            {
              "id":"9f977887-4265-4638-b0c1-0bcceea33ed7",
              "orderNumber":"ORD-0000001"
            }
            """);

        var @event = eventDeserializer.Deserialize(payload, nameof(OrderCreated), 0);

        @event.ShouldBeOfType<OrderCreated>();
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

    private static void RegisterProjectionSettings<TState>(IServiceCollection services, IConfiguration configuration, string sectionName)
        where TState : class, IModel, new()
    {
        var settings = configuration.GetRequiredSection(sectionName).Get<ProjectionSettings<TState>>()
                       ?? throw new InvalidOperationException($"Missing configuration section: {sectionName}");

        services.AddKeyedSingleton<IProjectionSettings<TState>>(GetRegistrationKey<TState>(sectionName), settings);
    }

    private static string GetRegistrationKey<TState>(string sectionName)
        where TState : class, IModel, new()
    {
        return ProjectionRegistration.GetKey<TState>(sectionName);
    }

    private sealed class TestModel : Model
    {
    }
}
