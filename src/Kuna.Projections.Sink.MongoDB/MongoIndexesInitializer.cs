using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class MongoIndexesInitializer : IProjectionStartupTask
{
    public Task RunAsync(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
