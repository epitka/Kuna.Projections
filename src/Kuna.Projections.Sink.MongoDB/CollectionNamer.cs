using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class CollectionNamer
{
    private readonly MongoProjectionOptions options;

    public CollectionNamer(MongoProjectionOptions options)
    {
        this.options = options;
    }

    public string GetModelCollectionName<TState>()
        where TState : class, IModel, new()
    {
        if (this.options.ModelCollectionNames.TryGetValue(typeof(TState), out string? collectionName))
        {
            return collectionName;
        }

        string normalizedTypeName = NormalizeTypeName(typeof(TState).Name);
        return $"{this.options.CollectionPrefix}_{normalizedTypeName}";
    }

    public string GetCheckpointCollectionName() => this.options.CheckpointCollectionName;

    public string GetFailureCollectionName() => this.options.FailureCollectionName;

    private static string NormalizeTypeName(string typeName)
    {
        string normalizedTypeName = typeName.EndsWith("Projection", StringComparison.Ordinal)
            ? typeName[..^"Projection".Length]
            : typeName;

        return string.Concat(
            normalizedTypeName.Select(
                (character, index) => index > 0 && char.IsUpper(character)
                    ? $"_{char.ToLowerInvariant(character)}"
                    : char.ToLowerInvariant(character).ToString()));
    }
}
