using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class InMemoryCheckpointStore : ICheckpointStore
{
    private CheckPoint checkPoint = new()
    {
        ModelName = ProjectionModelName.For<ItemModel>(),
        GlobalEventPosition = new GlobalEventPosition(0),
    };

    public Task<CheckPoint> GetCheckpoint(CancellationToken cancellationToken)
    {
        return Task.FromResult(this.checkPoint);
    }

    public Task PersistCheckpoint(CheckPoint checkPoint, CancellationToken cancellationToken)
    {
        this.checkPoint = checkPoint;
        return Task.CompletedTask;
    }
}
