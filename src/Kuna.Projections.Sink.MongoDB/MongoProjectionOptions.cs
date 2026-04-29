namespace Kuna.Projections.Sink.MongoDB;

public sealed class MongoProjectionOptions
{
    public string ConnectionString { get; set; } = string.Empty;

    public string DatabaseName { get; set; } = string.Empty;

    public string CollectionPrefix { get; set; } = "projection";

    public string CheckpointCollectionName { get; set; } = "projection_checkpoints";

    public string FailureCollectionName { get; set; } = "projection_failures";

    public IDictionary<Type, string> ModelCollectionNames { get; } = new Dictionary<Type, string>();
}
