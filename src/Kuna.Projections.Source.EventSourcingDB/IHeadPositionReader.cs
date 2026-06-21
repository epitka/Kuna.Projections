using EventSourcingDb;
using EventSourcingDb.Types;
using Microsoft.Extensions.Logging;

namespace Kuna.Projections.Source.EventSourcingDB;

/// <summary>
/// Reads the current head (latest) global event id within a subject scope. Used to
/// decide when a catch-up replay has reached the live tail, since EventSourcingDB
/// exposes no explicit caught-up marker on the observe stream.
/// </summary>
public interface IHeadPositionReader
{
    /// <summary>
    /// Returns the id of the latest event in the configured subject scope, or
    /// <see langword="null"/> when the scope contains no events.
    /// </summary>
    Task<string?> ReadHeadPositionAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Default <see cref="IHeadPositionReader"/> that determines the head id by reading
/// the configured subject antichronologically and taking the first event. This is
/// the only mechanism available today; a dedicated server-side endpoint is planned,
/// at which point only this reader needs to change.
/// </summary>
public sealed class EventSourcingDbHeadPositionReader : IHeadPositionReader
{
    private readonly IClient client;
    private readonly string subject;
    private readonly bool recursive;

    /// <summary>
    /// Initializes the head-position reader for the given subject scope.
    /// </summary>
    public EventSourcingDbHeadPositionReader(IClient client, string subject, bool recursive)
    {
        this.client = client;
        this.subject = subject;
        this.recursive = recursive;
    }

    /// <summary>
    /// Reads the head event id within the configured subject scope.
    /// </summary>
    public async Task<string?> ReadHeadPositionAsync(CancellationToken cancellationToken)
    {
        var options = new ReadEventsOptions(
            Recursive: this.recursive,
            Order: Order.Antichronological);

        await foreach (var sourceEvent in this.client.ReadEventsAsync(this.subject, options, cancellationToken))
        {
            return sourceEvent.Id;
        }

        return null;
    }
}
