using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

public class DeserializationFailed : Event
{
    public GlobalEventPosition GlobalEventPosition { get; set; }

    public long EventNumber { get; set; }

    public Guid ModelId { get; set; }
}
