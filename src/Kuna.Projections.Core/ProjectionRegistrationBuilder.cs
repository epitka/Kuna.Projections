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
}
