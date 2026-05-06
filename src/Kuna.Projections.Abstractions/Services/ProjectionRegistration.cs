using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Computes the DI key used to isolate one projection definition from another.
/// </summary>
public static class ProjectionRegistration
{
    public static string GetKey<TState>(string settingsSectionName)
        where TState : class, IModel, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(settingsSectionName);

        var typeName = typeof(TState).FullName ?? typeof(TState).Name;
        return $"{typeName}::{settingsSectionName.Trim()}";
    }
}
