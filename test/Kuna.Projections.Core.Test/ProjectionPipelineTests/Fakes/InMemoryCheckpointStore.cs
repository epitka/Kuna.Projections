using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private readonly Dictionary<string, CheckPoint> checkPoints = new();

    public Task<CheckPoint> GetCheckpoint(string modelName, CancellationToken cancellationToken)
    {
        if (this.checkPoints.TryGetValue(modelName, out var checkPoint))
        {
            return Task.FromResult(checkPoint);
        }

        return Task.FromResult(
            new CheckPoint
            {
                ModelName = modelName,
                GlobalEventPosition = new GlobalEventPosition(string.Empty),
            });
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        this.checkPoints[checkPoint.ModelName] = checkPoint;
        return Task.CompletedTask;
    }
}
