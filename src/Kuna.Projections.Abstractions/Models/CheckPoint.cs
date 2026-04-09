namespace Kuna.Projections.Abstractions.Models;

/// <summary>
/// Stores the last persisted global event position for a projection model so
/// processing can resume from the correct point after restart.
/// </summary>
public class CheckPoint
{
    public required string ModelName { get; init; }

    public GlobalEventPosition GlobalEventPosition { get; set; }
}
