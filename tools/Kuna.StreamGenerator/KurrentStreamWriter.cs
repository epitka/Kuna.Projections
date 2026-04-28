using KurrentDB.Client;

namespace Kuna.StreamGenerator;

public sealed record StreamWriteEvent(string StreamName, string EventTypeName, object EventData);

public sealed class StreamWritePlan
{
    public StreamWritePlan(IReadOnlyList<StreamWriteEvent> events)
    {
        this.Events = events;
    }

    public IReadOnlyList<StreamWriteEvent> Events { get; }

    public int TotalEvents => this.Events.Count;
}

public static class KurrentStreamWriter
{
    public static async Task WriteAsync(
        StreamWritePlan plan,
        string connectionString,
        IProgress<StreamWriteProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var client = new KurrentDBClient(KurrentDBClientSettings.Create(connectionString));
        var seenStreams = new HashSet<string>(StringComparer.Ordinal);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var written = 0;

        foreach (var item in plan.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var payload = EventStoreJson.Serialize(item.EventData);
            var eventData = new EventData(
                Uuid.NewUuid(),
                item.EventTypeName,
                payload);

            var state = seenStreams.Add(item.StreamName)
                            ? StreamState.NoStream
                            : StreamState.Any;

            await client.AppendToStreamAsync(
                item.StreamName,
                state,
                new[] { eventData, },
                cancellationToken: cancellationToken);

            written++;

            progress?.Report(
                new StreamWriteProgress(
                    written,
                    plan.TotalEvents,
                    stopwatch.Elapsed));
        }
    }
}

public sealed record StreamWriteProgress(int WrittenEvents, int TotalEvents, TimeSpan Elapsed);
