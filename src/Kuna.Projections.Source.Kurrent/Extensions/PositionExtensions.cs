using Kuna.Projections.Abstractions.Models;
using KurrentDB.Client;

namespace Kuna.Projections.Source.Kurrent.Extensions;

/// <summary>
/// Converts Kurrent position values into projection checkpoint types.
/// </summary>
public static class PositionExtensions
{
    /// <summary>
    /// Converts a Kurrent position into a projection global event position.
    /// </summary>
    public static GlobalEventPosition ToGlobalEventPosition(this Position position)
    {
        return new GlobalEventPosition(position.CommitPosition);
    }
}
