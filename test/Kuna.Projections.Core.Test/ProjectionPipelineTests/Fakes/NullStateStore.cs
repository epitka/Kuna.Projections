using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ProjectionPipelineTests.Fakes;

internal sealed class NullStateStore : IModelStateStore<ItemModel>
{
    public Task<ItemModel?> Load(Guid modelId, CancellationToken cancellationToken)
    {
        return Task.FromResult<ItemModel?>(null);
    }
}
