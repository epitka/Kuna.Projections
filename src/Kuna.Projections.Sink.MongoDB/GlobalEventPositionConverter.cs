using System.Globalization;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Sink.MongoDB;

internal static class GlobalEventPositionConverter
{
    public static string Format(GlobalEventPosition value)
    {
        return value.Value.ToString(CultureInfo.InvariantCulture);
    }

    public static GlobalEventPosition Parse(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        if (!ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedValue))
        {
            throw new FormatException($"Could not parse GlobalEventPosition from '{value}'.");
        }

        return new GlobalEventPosition(parsedValue);
    }
}
