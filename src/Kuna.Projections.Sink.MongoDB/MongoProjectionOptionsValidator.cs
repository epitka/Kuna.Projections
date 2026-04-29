namespace Kuna.Projections.Sink.MongoDB;

internal static class MongoProjectionOptionsValidator
{
    public static void Validate(MongoProjectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.DatabaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CollectionPrefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CheckpointCollectionName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.FailureCollectionName);
    }
}
