namespace Kuna.Projections.Abstractions.Models;

/// <summary>
/// Internal control event emitted by projection sources when catch-up is reached.
/// </summary>
public sealed class ProjectionCaughtUpEvent : Event
{
    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    public ProjectionCaughtUpEvent()
    {
        this.TypeName = nameof(ProjectionCaughtUpEvent);
        this.CreatedOn = DateTime.UtcNow;
    }
}
