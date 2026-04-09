namespace Kuna.Projections.Abstractions.Models;

public readonly record struct GlobalEventPosition(ulong Value)
{
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
        return this.Value.ToString();
    }
}
