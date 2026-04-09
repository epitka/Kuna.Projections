namespace Kuna.Projections.Abstractions.Models;

public enum EventVersionCheckStrategy
{
    Disabled = 0,
    Consecutive = 1,
    Monotonic = 2,
}
