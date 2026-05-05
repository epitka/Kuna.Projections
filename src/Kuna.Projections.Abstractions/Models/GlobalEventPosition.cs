namespace Kuna.Projections.Abstractions.Models;

public readonly record struct GlobalEventPosition(string Value)
{
    public override string ToString()
    {
        return this.Value;
    }
}
