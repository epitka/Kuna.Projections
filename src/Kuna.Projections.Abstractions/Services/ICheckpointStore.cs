using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

/// <summary>
/// Provides storage for reading and updating projection checkpoints.
/// </summary>
public interface ICheckpointStore
{
    Task<CheckPoint> GetCheckpoint(string modelName, CancellationToken cancellationToken);

    Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken);
}
