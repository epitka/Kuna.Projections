using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class NoOpFailureHandler : IProjectionFailureHandler<ItemModel>
{
    public Task Handle(ProjectionFailure failure, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
