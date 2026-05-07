using Kuna.Projections.Abstractions.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Represents one projection-definition registration under construction.
/// </summary>
public interface IProjectionRegistrationBuilder<TState>
    where TState : class, IModel, new()
{
    IServiceCollection Services { get; }

    IConfiguration Configuration { get; }

    string SettingsSectionName { get; }

    string RegistrationKey { get; }
}
