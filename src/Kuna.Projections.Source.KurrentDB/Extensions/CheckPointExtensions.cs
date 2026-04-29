using Kuna.Projections.Abstractions.Models;
using KurrentDB.Client;

namespace Kuna.Projections.Source.KurrentDB.Extensions;

/// <summary>
/// Converts projection checkpoint values into Kurrent position types.
/// </summary>
public static class CheckPointExtensions
{
    /// <summary>
    /// Converts a projection checkpoint into the corresponding Kurrent global
    /// position.
    /// </summary>
    public static Position ToKurrentDbPosition(this CheckPoint checkPoint)
    {
        return new Position(checkPoint.GlobalEventPosition.Value, checkPoint.GlobalEventPosition.Value);
    }
}
