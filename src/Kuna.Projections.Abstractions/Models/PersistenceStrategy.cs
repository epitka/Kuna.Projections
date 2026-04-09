namespace Kuna.Projections.Abstractions.Models;

public enum PersistenceStrategy
{
    ModelCountBatching,
    TimeBasedBatching,
    ImmediateModelFlush,
}
