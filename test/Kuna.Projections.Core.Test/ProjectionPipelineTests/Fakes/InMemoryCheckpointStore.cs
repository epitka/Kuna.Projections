using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly Dictionary<(string ModelName, string InstanceId), CheckPoint> checkPoints = new();

    public Task<CheckPoint> GetCheckpoint(string modelName, string instanceId, CancellationToken cancellationToken)
    {
        if (this.checkPoints.TryGetValue((modelName, instanceId), out var checkPoint))
        {
            return Task.FromResult(checkPoint);
        }

        return Task.FromResult(
            new CheckPoint
            {
                ModelName = modelName,
                InstanceId = instanceId,
                GlobalEventPosition = new GlobalEventPosition(string.Empty),
            });
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        this.checkPoints[(checkPoint.ModelName, checkPoint.InstanceId)] = checkPoint;
        return Task.CompletedTask;
    }
}
