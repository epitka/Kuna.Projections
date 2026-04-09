using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Messages;

/// <summary>
/// Represents one sink flush of projection model states.
/// </summary>
public sealed record ModelStatesBatch<TState>
{
    /// <summary>
    /// Model states to persist in this flush.
    /// </summary>
    public required IReadOnlyList<ModelState<TState>> Changes { get; init; }

    /// <summary>
    /// Highest global event position covered by this flush.
    /// After the batch is written, the checkpoint can advance to this position.
    /// </summary>
    public required GlobalEventPosition GlobalEventPosition { get; init; }
}
