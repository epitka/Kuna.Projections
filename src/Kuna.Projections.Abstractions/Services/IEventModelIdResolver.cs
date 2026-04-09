using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

public interface IEventModelIdResolver
{
    bool TryResolve(Event @event, string streamId, out Guid modelId);
}
