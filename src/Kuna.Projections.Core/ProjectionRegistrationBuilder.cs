using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Core;

public sealed class ProjectionRegistrationBuilder<TState>
    : IProjectionRegistrationBuilder<TState>
    where TState : class, IModel, new()
{
    internal ProjectionRegistrationBuilder(
        IServiceCollection services,
        IConfiguration configuration,
        string settingsSectionName,
        string registrationKey)
    {
        this.Services = services;
        this.Configuration = configuration;
        this.SettingsSectionName = settingsSectionName;
        this.RegistrationKey = registrationKey;
    }

    public IServiceCollection Services { get; }

    public IConfiguration Configuration { get; }

    public string SettingsSectionName { get; }

    public string RegistrationKey { get; }

    public IProjectionRegistrationBuilder<TState> WithInitialEvent<TEvent>()
        where TEvent : Event
    {
        var existingRegistration = this.Services
                                       .Where(
                                           x => x.ServiceType == typeof(ProjectionCreationRegistration<TState>)
                                                && Equals(x.ServiceKey, this.RegistrationKey))
                                       .Select(
                                           x => x.IsKeyedService
                                               ? x.KeyedImplementationInstance
                                               : x.ImplementationInstance)
                                       .OfType<ProjectionCreationRegistration<TState>>()
                                       .LastOrDefault();

        if (existingRegistration != null)
        {
            throw new InvalidOperationException(
                $"Projection {typeof(TState).FullName} already has initial event {existingRegistration.InitialEventType.FullName} configured.");
        }

        this.Services.AddKeyedSingleton(this.RegistrationKey, new ProjectionCreationRegistration<TState>(typeof(TEvent)));

        return this;
    }
}
