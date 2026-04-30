namespace Kuna.Projections.Sink.MongoDB;

public sealed class ProjectionOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    public string DatabaseName { get; set; } = string.Empty;

    public string CollectionPrefix { get; set; } = "projection";

    public string CheckpointCollectionName => "projection_checkpoints";

    public string FailureCollectionName => "projection_failures";

    public IDictionary<Type, string> ModelCollectionNames { get; } = new Dictionary<Type, string>();

    public void SetModelCollectionName<TState>(string collectionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        this.ModelCollectionNames[typeof(TState)] = collectionName;
    }
}
