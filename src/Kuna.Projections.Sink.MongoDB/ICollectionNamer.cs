using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Sink.MongoDB;

public interface ICollectionNamer
{
    string GetModelCollectionName<TState>()
        where TState : class, IModel, new();

    string GetCheckpointCollectionName();
}

public sealed class CollectionNamer : ICollectionNamer
{
    private readonly ProjectionOptions options;

    public CollectionNamer(ProjectionOptions options)
    {
        this.options = options;
    }

    public string GetModelCollectionName<TState>()
        where TState : class, IModel, new()
    {
        if (this.options.ModelCollectionNames.TryGetValue(typeof(TState), out var collectionName))
        {
            return collectionName;
        }

        var normalizedTypeName = NormalizeTypeName(typeof(TState).Name);
        return $"{this.options.CollectionPrefix}_{normalizedTypeName}";
    }

    public string GetCheckpointCollectionName()
    {
        return this.options.CheckpointCollectionName;
    }

    private static string NormalizeTypeName(string typeName)
    {
        var normalizedTypeName = typeName.EndsWith("Projection", StringComparison.Ordinal)
                                     ? typeName[..^"Projection".Length]
                                     : typeName;

        return string.Concat(
            normalizedTypeName.Select(
                (character, index) => index > 0 && char.IsUpper(character)
                                          ? $"_{char.ToLowerInvariant(character)}"
                                          : char.ToLowerInvariant(character).ToString()));
    }
}
