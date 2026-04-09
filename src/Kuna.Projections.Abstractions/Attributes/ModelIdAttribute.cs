namespace Kuna.Projections.Abstractions.Attributes;

/// <summary>
/// Marks the event property that contains the projection model identifier.
/// Implementations of <c>Kuna.Projections.Core/IEventModelIdResolver.cs</c>
/// can use this attribute to extract the model id from the event.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ModelIdAttribute : Attribute
{
}
