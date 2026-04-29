using System.Globalization;

namespace Kuna.Projections.Sink.MongoDB;

internal static class MongoGuid
{
    public static string Format(Guid value)
    {
        return value.ToString("D", CultureInfo.InvariantCulture);
    }

    public static Guid Parse(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return Guid.ParseExact(value, "D");
    }
}
