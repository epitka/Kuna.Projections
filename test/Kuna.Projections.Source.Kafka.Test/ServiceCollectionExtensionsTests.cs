using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Source.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Source.Kafka.Test;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddKafkaSource_Should_Throw_When_Kafka_Section_Is_Missing()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
            });

        services.AddKafkaSource<TestModel>(
            configuration,
            LoggerFactory.Create(_ =>
            {
            }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain("Projections:Kafka");
    }

    [Fact]
    public void AddKafkaSource_Should_Throw_When_Partitions_Contain_Duplicates()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:BootstrapServers"] = "localhost:9092",
                ["Projections:Kafka:Topic"] = "orders-events",
                ["Projections:Kafka:Partitions:0"] = "1",
                ["Projections:Kafka:Partitions:1"] = "1",
            });

        services.AddKafkaSource<TestModel>(
            configuration,
            LoggerFactory.Create(_ =>
            {
            }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        var ex = Should.Throw<InvalidOperationException>(
            () => provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)));

        ex.Message.ShouldContain("duplicate partition ids");
    }

    [Fact]
    public void AddKafkaSource_Should_Register_Services()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:BootstrapServers"] = "localhost:9092",
                ["Projections:Kafka:Topic"] = "orders-events",
            });

        services.AddKafkaSource<TestModel>(
            configuration,
            LoggerFactory.Create(_ =>
            {
            }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)).ShouldNotBeNull();
        provider.GetRequiredService<ICheckpointSerializer<KafkaCheckpointDocument>>().ShouldBeOfType<KafkaCheckpointSerializer>();
    }

    private static IConfiguration BuildConfiguration(IDictionary<string, string?> values)
    {
        return new ConfigurationBuilder()
               .AddInMemoryCollection(values)
               .Build();
    }

    private static void RegisterProjectionSettings<TState>(
        IServiceCollection services,
        IConfiguration configuration,
        string settingsSectionName)
        where TState : class, IModel, new()
    {
        var registrationKey = GetRegistrationKey<TState>(settingsSectionName);
        var settings = configuration.GetRequiredSection(settingsSectionName).Get<ProjectionSettings<TState>>()
                       ?? new ProjectionSettings<TState>();

        services.AddKeyedSingleton<IProjectionSettings<TState>>(registrationKey, settings);
    }

    private static string GetRegistrationKey<TState>(string settingsSectionName)
        where TState : class, IModel, new()
    {
        return ProjectionRegistration.GetKey<TState>(settingsSectionName);
    }

    private sealed class TestModel : Model
    {
    }
}
