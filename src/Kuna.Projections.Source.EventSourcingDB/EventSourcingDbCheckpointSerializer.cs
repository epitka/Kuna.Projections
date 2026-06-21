using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Converts between an EventSourcingDB event id and the framework
/// <see cref="GlobalEventPosition"/>. EventSourcingDB exposes the global position
/// as a monotonic integer rendered as a string, so the conversion is an identity
/// apart from normalizing the "from the beginning" sentinel.
/// </summary>
public sealed class EventSourcingDbCheckpointSerializer : ICheckpointSerializer<string>
{
    public GlobalEventPosition Serialize(string checkpoint)
    {
        return new GlobalEventPosition(checkpoint ?? string.Empty);
    }

    public string Deserialize(GlobalEventPosition checkpoint)
    {
        if (string.IsNullOrWhiteSpace(checkpoint.Value)
            || checkpoint.Value == "0")
        {
            return string.Empty;
        }

        return checkpoint.Value;
    }
}
