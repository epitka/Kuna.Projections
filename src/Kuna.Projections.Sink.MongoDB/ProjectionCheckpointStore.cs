using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Sink.MongoDB;

internal sealed class ProjectionCheckpointStore : ICheckpointStore
{
    public Task<CheckPoint> GetCheckpoint(CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        throw new NotImplementedException();
    }
}
