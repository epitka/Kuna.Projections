namespace Kuna.Projections.Sink.MongoDB;

internal static class ProjectionOptionsValidator
{
    public static void Validate(ProjectionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.DatabaseName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CollectionPrefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.CheckpointCollectionName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.FailureCollectionName);
    }
}
