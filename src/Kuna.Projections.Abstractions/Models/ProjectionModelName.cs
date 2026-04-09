namespace Kuna.Projections.Abstractions.Models;

/// <summary>
/// Provides the stable projection model identifier used by shared infrastructure records.
/// </summary>
public static class ProjectionModelName
{
    public static string For<TModel>() => For(typeof(TModel));

    public static string For(Type modelType)
    {
        return modelType.FullName
               ?? throw new InvalidOperationException($"Model type {modelType} does not have a full name.");
    }
}
