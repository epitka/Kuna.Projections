using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using KurrentDB.Client;

namespace Kuna.Projections.Source.KurrentDB;

public sealed class KurrentDbCheckpointSerializer : ICheckpointSerializer<Position>
{
    public GlobalEventPosition Serialize(Position checkpoint)
    {
        return new GlobalEventPosition(checkpoint.ToString());
    }

    public Position Deserialize(GlobalEventPosition checkpoint)
    {
        if (string.IsNullOrEmpty(checkpoint.Value)
            || checkpoint.Value == "0")
        {
            return Position.Start;
        }

        if (!Position.TryParse(checkpoint.Value, out var position)
            || position is null)
        {
            throw new FormatException($"Invalid KurrentDB checkpoint '{checkpoint.Value}'.");
        }

        return position.Value;
    }
}
