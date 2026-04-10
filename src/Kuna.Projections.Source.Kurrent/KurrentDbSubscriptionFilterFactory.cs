using KurrentDB.Client;

namespace Kuna.Projections.Source.Kurrent;

internal static class KurrentDbSubscriptionFilterFactory
{
    internal static SubscriptionFilterOptions Create(KurrentDbFilterSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return settings.Kind switch
        {
            KurrentDbFilterKind.StreamPrefix => new SubscriptionFilterOptions(
                StreamFilter.Prefix(GetSinglePrefix(settings))),
            _ => throw new ArgumentOutOfRangeException(nameof(settings.Kind)),
        };
    }

    internal static string GetSinglePrefix(KurrentDbFilterSettings settings)
    {
        if (settings.Prefixes.Length != 1
            || string.IsNullOrWhiteSpace(settings.Prefixes[0]))
        {
            throw new InvalidOperationException("Prefix-based KurrentDB filter kinds require exactly one non-empty prefix.");
        }

        return settings.Prefixes[0];
    }
}
