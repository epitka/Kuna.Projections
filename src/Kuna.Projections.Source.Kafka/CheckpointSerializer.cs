using System.Text.Json;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;

namespace Kuna.Projections.Source.Kafka;

public sealed class CheckpointSerializer : ICheckpointSerializer<Checkpoint>
{
    private static readonly JsonSerializerOptions JsonSerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public GlobalEventPosition Serialize(Checkpoint checkpoint)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        var normalizedCheckpoint = new Checkpoint
        {
            Topic = checkpoint.Topic,
            Partitions = checkpoint.Partitions
                                   .OrderBy(x => x.Key)
                                   .ToDictionary(x => x.Key, x => x.Value),
        };

        return GlobalEventPosition.From(JsonSerializer.Serialize(normalizedCheckpoint, JsonSerializerOptions));
    }

    public Checkpoint Deserialize(GlobalEventPosition checkpoint)
    {
        if (string.IsNullOrWhiteSpace(checkpoint.Value))
        {
            return new Checkpoint();
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<Checkpoint>(checkpoint.Value, JsonSerializerOptions);

            if (parsed is null)
            {
                throw new FormatException("Checkpoint payload was empty.");
            }

            if (parsed.Topic is null)
            {
                throw new FormatException("Checkpoint topic is required.");
            }

            if (parsed.Partitions is null)
            {
                throw new FormatException("Checkpoint partitions are required.");
            }

            if (parsed.Partitions.Keys.Any(partition => partition < 0))
            {
                throw new FormatException("Checkpoint partitions must be non-negative.");
            }

            return new Checkpoint
            {
                Topic = parsed.Topic,
                Partitions = parsed.Partitions
                                   .OrderBy(x => x.Key)
                                   .ToDictionary(x => x.Key, x => x.Value),
            };
        }
        catch (JsonException ex)
        {
            throw new FormatException($"Invalid Kafka checkpoint '{checkpoint.Value}'.", ex);
        }
        catch (FormatException ex)
        {
            throw new FormatException($"Invalid Kafka checkpoint '{checkpoint.Value}': {ex.Message}", ex);
        }
    }
}
