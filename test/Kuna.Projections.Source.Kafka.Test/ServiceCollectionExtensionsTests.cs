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

        var ex = Should.Throw<InvalidOperationException>(
            () => services.AddKafkaSource<TestModel>(
                configuration,
                LoggerFactory.Create(
                    _ =>
                    {
                    }),
                ProjectionSettingsSection.Name));

        ex.Message.ShouldContain("Projections:Kafka");
    }

    [Fact]
    public void AddKafkaSource_Should_Throw_When_Kafka_Connection_String_Is_Missing()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:Topic"] = "orders-events",
            });

        var ex = Should.Throw<InvalidOperationException>(
            () => services.AddKafkaSource<TestModel>(
                configuration,
                LoggerFactory.Create(
                    _ =>
                    {
                    }),
                ProjectionSettingsSection.Name));

        ex.Message.ShouldContain("Missing connection string: Kafka");
    }

    [Fact]
    public void AddKafkaSource_Should_Throw_When_Partitions_Contain_Duplicates()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["ConnectionStrings:Kafka"] = "localhost:9092",
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:Topic"] = "orders-events",
                ["Projections:Kafka:Partitions:0"] = "1",
                ["Projections:Kafka:Partitions:1"] = "1",
            });

        var ex = Should.Throw<InvalidOperationException>(
            () => services.AddKafkaSource<TestModel>(
                configuration,
                LoggerFactory.Create(
                    _ =>
                    {
                    }),
                ProjectionSettingsSection.Name));

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
                ["ConnectionStrings:Kafka"] = "localhost:9092",
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:Topic"] = "orders-events",
            });

        services.AddKafkaSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        RegisterProjectionSettings<TestModel>(services, configuration, ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IProjectionEventSource<TestModel>>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name)).ShouldNotBeNull();
        provider.GetRequiredKeyedService<IKafkaSourceTransformer>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name))
                .ShouldBeOfType<KunaKafkaSourceTransformer>();

        provider.GetRequiredService<ICheckpointSerializer<KafkaCheckpointDocument>>().ShouldBeOfType<KafkaCheckpointSerializer>();
        provider.GetRequiredService<IKafkaEventDeserializer>().ShouldBeOfType<KafkaEventDeserializer>();
    }

    [Fact]
    public void AddKafkaSource_Should_Not_Replace_Custom_Transformer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["ConnectionStrings:Kafka"] = "localhost:9092",
                ["Projections:Source"] = "Kafka",
                ["Projections:InstanceId"] = "orders-v1",
                ["Projections:Kafka:Topic"] = "orders-events",
            });

        services.AddKeyedSingleton<IKafkaSourceTransformer, CustomKafkaSourceTransformer>(
            GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name));

        services.AddKafkaSource<TestModel>(
            configuration,
            LoggerFactory.Create(
                _ =>
                {
                }),
            ProjectionSettingsSection.Name);

        using var provider = services.BuildServiceProvider();

        provider.GetRequiredKeyedService<IKafkaSourceTransformer>(GetRegistrationKey<TestModel>(ProjectionSettingsSection.Name))
                .ShouldBeOfType<CustomKafkaSourceTransformer>();
    }

    [Fact]
    public void AddKafkaSource_Should_Bind_Optional_Consumer_Group_Id()
    {
        var configuration = BuildConfiguration(
            new Dictionary<string, string?>
            {
                ["ConnectionStrings:Kafka"] = "localhost:9092",
                ["Projections:Kafka:ConsumerGroupId"] = "orders-consumer",
                ["Projections:Kafka:Topic"] = "orders-events",
            });

        var settings = KafkaSourceSettingsResolver.Resolve(configuration, ProjectionSettingsSection.Name);

        settings.ConsumerGroupId.ShouldBe("orders-consumer");
        settings.BootstrapServers.ShouldBe("localhost:9092");
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

    private sealed class CustomKafkaSourceTransformer : IKafkaSourceTransformer
    {
        public KafkaSourceRecord Transform(KafkaSourceRecordContext context)
        {
            throw new NotSupportedException();
        }
    }
}
