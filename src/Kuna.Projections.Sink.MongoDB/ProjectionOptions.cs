namespace Kuna.Projections.Sink.MongoDB;

public sealed class ProjectionOptions
{
    private string collectionPrefix = "projection";

    public ProjectionOptions(string connectionString, string databaseName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(databaseName);

        this.ConnectionString = connectionString;
        this.DatabaseName = databaseName;
    }

    public string ConnectionString { get; }

    public string DatabaseName { get; }

    public string CollectionPrefix
    {
        get => this.collectionPrefix;
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            this.collectionPrefix = value;
        }
    }

    public string CheckpointCollectionName => "projection_checkpoints";

    public IDictionary<Type, string> ModelCollectionNames { get; } = new Dictionary<Type, string>();

    public void SetModelCollectionName<TState>(string collectionName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectionName);
        this.ModelCollectionNames[typeof(TState)] = collectionName;
    }
}
