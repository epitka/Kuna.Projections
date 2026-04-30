using System.Globalization;

namespace Kuna.Projections.Abstractions.Models;

public readonly record struct GlobalEventPosition(ulong Value)
{
    public static GlobalEventPosition From(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);

        return !ulong.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsedValue)
                   ? throw new FormatException($"Could not parse GlobalEventPosition from '{value}'.")
                   : new GlobalEventPosition(parsedValue);
    }

    public static implicit operator GlobalEventPosition(ulong value)
    {
        return new GlobalEventPosition(value);
    }

    public static implicit operator ulong(GlobalEventPosition value)
    {
        return value.Value;
    }

    public override string ToString()
    {
        return this.Value.ToString(CultureInfo.InvariantCulture);
    }
}
