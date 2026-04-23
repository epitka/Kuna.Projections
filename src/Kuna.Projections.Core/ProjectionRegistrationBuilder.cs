using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Kuna.Projections.Core;

public sealed class ProjectionRegistrationBuilder<TState>
    where TState : class, IModel, new()
{
    private readonly IServiceCollection services;

    internal ProjectionRegistrationBuilder(IServiceCollection services)
    {
        this.services = services;
    }

    public ProjectionRegistrationBuilder<TState> WithInitialEvent<TEvent>()
        where TEvent : Event
    {
        var existingRegistration = this.services
                                       .Where(x => x.ServiceType == typeof(ProjectionCreationRegistration<TState>))
                                       .Select(x => x.ImplementationInstance)
                                       .OfType<ProjectionCreationRegistration<TState>>()
                                       .LastOrDefault();

        if (existingRegistration != null)
        {
            throw new InvalidOperationException(
                $"Projection {typeof(TState).FullName} already has initial event {existingRegistration.InitialEventType.FullName} configured.");
        }

        this.services.Replace(ServiceDescriptor.Singleton(new ProjectionCreationRegistration<TState>(typeof(TEvent))));

        return this;
    }
}
