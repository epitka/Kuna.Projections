using System.Globalization;

namespace Kuna.Projections.Abstractions.Models;

public readonly record struct GlobalEventPosition(string Value)
{
    public GlobalEventPosition(long value)
        : this(value.ToString(CultureInfo.InvariantCulture))
    {
    }

    public static GlobalEventPosition From(string value)
    {
        if (value == string.Empty)
        {
            return new GlobalEventPosition(string.Empty);
        }

        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return new GlobalEventPosition(value);
    }

    public override string ToString()
    {
        return this.Value;
    }
}
