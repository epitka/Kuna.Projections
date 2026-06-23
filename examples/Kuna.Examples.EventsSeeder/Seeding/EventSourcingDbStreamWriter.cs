using EventSourcingDb;
using Kuna.StreamGenerator;
using EventCandidate = EventSourcingDb.Types.EventCandidate;

namespace Kuna.Examples.EventsSeeder.Seeding;

public sealed record EventSourcingDbWritePlan(IReadOnlyList<InterleavedOrderEvent> Events);

public static class EventSourcingDbStreamWriter
{
    private const string EventSourceUri = "https://github.com/thenativeweb/kuna-projections";
    private const string SubjectRoot = "/orders";
    private const string EventTypePrefix = "io.kuna.orders.";
    private const int DefaultBatchSize = 500;

    public static async Task WriteAsync(
        EventSourcingDbWritePlan plan,
        string baseUrl,
        string apiToken,
        int batchSize = DefaultBatchSize,
        IProgress<StreamWriteProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var effectiveBatchSize = batchSize < 1 ? DefaultBatchSize : batchSize;
        var client = new Client(new Uri(baseUrl), apiToken);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var written = 0;
        var batch = new List<EventCandidate>(effectiveBatchSize);

        foreach (var item in plan.Events)
        {
            cancellationToken.ThrowIfCancellationRequested();

            batch.Add(ToCandidate(item));

            if (batch.Count < effectiveBatchSize)
            {
                continue;
            }

            written = await FlushAsync(client, batch, written, plan.Events.Count, stopwatch, progress, cancellationToken);
        }

        if (batch.Count > 0)
        {
            await FlushAsync(client, batch, written, plan.Events.Count, stopwatch, progress, cancellationToken);
        }
    }

    /// <summary>
    /// Polls the EventSourcingDB ping endpoint until it responds or the timeout
    /// elapses. The container reports "started" before the database is actually
    /// accepting connections, so seeding right after <c>docker compose up</c> can
    /// otherwise fail mid-run with a raw "connection refused". Returns
    /// <see langword="true"/> once the server is reachable, <see langword="false"/>
    /// if it is still unreachable after <paramref name="timeout"/>.
    /// </summary>
    public static async Task<bool> WaitUntilReadyAsync(
        string baseUrl,
        string apiToken,
        TimeSpan timeout,
        Action<string>? log = null,
        CancellationToken cancellationToken = default)
    {
        var client = new Client(new Uri(baseUrl), apiToken);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var announced = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await client.PingAsync(cancellationToken);
                return true;
            }
            catch (Exception) when (stopwatch.Elapsed < timeout)
            {
                if (!announced)
                {
                    log?.Invoke($"Waiting for EventSourcingDB at {baseUrl} to become ready...");
                    announced = true;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            }
            catch (Exception)
            {
                return false;
            }
        }
    }

    private static async Task<int> FlushAsync(
        Client client,
        List<EventCandidate> batch,
        int written,
        int totalEvents,
        System.Diagnostics.Stopwatch stopwatch,
        IProgress<StreamWriteProgress>? progress,
        CancellationToken cancellationToken)
    {
        await client.WriteEventsAsync(batch, token: cancellationToken);

        written += batch.Count;
        batch.Clear();

        progress?.Report(
            new StreamWriteProgress(
                written,
                totalEvents,
                stopwatch.Elapsed));

        return written;
    }

    private static EventCandidate ToCandidate(InterleavedOrderEvent item)
    {
        var modelId = ResolveModelId(item.StreamName);

        item.Event.TypeName = string.IsNullOrWhiteSpace(item.Event.TypeName)
                                  ? item.Event.GetType().Name
                                  : item.Event.TypeName;

        return new EventCandidate(
            EventSourceUri,
            $"{SubjectRoot}/{modelId:N}",
            $"{EventTypePrefix}{item.Event.TypeName}",
            item.Event);
    }

    private static Guid ResolveModelId(string streamName)
    {
        var separatorIndex = streamName.IndexOf('-', StringComparison.Ordinal);

        if (separatorIndex >= 0
            && separatorIndex < streamName.Length - 1
            && Guid.TryParse(streamName[(separatorIndex + 1)..], out var modelId))
        {
            return modelId;
        }

        throw new InvalidOperationException($"Stream '{streamName}' does not contain a Guid model id suffix.");
    }
}
