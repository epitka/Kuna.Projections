using KurrentDB.Client;

namespace Kuna.Projections.Source.KurrentDB;

public static class KurrentDbSubscriptionFilterFactory
{
    public static SubscriptionFilterOptions Create(KurrentDbFilterSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        return settings.Kind switch
               {
                   KurrentDbFilterKind.StreamPrefix    => new SubscriptionFilterOptions(StreamFilter.Prefix(GetPrefixes(settings))),
                   KurrentDbFilterKind.StreamRegex     => new SubscriptionFilterOptions(StreamFilter.RegularExpression(GetRegex(settings))),
                   KurrentDbFilterKind.EventTypePrefix => new SubscriptionFilterOptions(EventTypeFilter.Prefix(GetPrefixes(settings))),
                   KurrentDbFilterKind.EventTypeRegex  => new SubscriptionFilterOptions(EventTypeFilter.RegularExpression(GetRegex(settings))),
                   _                                   => throw new ArgumentOutOfRangeException(nameof(settings.Kind)),
               };
    }

    public static string[] GetPrefixes(KurrentDbFilterSettings settings)
    {
        if (settings.Prefixes.Length == 0
            || settings.Prefixes.Any(string.IsNullOrWhiteSpace))
        {
            throw new InvalidOperationException("Prefix-based KurrentDB filter kinds require at least one non-empty prefix.");
        }

        return settings.Prefixes;
    }

    public static string GetRegex(KurrentDbFilterSettings settings)
    {
        if (string.IsNullOrWhiteSpace(settings.Regex))
        {
            throw new InvalidOperationException("Regex-based KurrentDB filter kinds require a non-empty regular expression.");
        }

        return settings.Regex;
    }
}
