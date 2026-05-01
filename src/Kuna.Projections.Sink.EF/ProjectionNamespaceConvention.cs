using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Kuna.Projections.Sink.EF;

internal static class ProjectionNamespaceConvention
{
    internal static string? Normalize(string? projectionNamespace)
    {
        return string.IsNullOrWhiteSpace(projectionNamespace)
                   ? null
                   : projectionNamespace.Trim();
    }

    internal static string? GetSchema(string? projectionNamespace, string? providerName)
    {
        var normalized = Normalize(projectionNamespace);
        return normalized != null && UsesSchema(providerName)
                   ? normalized
                   : null;
    }

    internal static void Apply(ModelBuilder modelBuilder, string? projectionNamespace, string? providerName)
    {
        var normalized = Normalize(projectionNamespace);

        if (normalized == null)
        {
            return;
        }

        if (UsesSchema(providerName))
        {
            modelBuilder.HasDefaultSchema(normalized);
            return;
        }

        ApplyPrefix(modelBuilder, normalized);
    }

    private static bool UsesSchema(string? providerName)
    {
        if (string.IsNullOrWhiteSpace(providerName))
        {
            return true;
        }

        return providerName.Contains("Npgsql", StringComparison.OrdinalIgnoreCase)
               || providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase);
    }

    private static void ApplyPrefix(ModelBuilder modelBuilder, string projectionNamespace)
    {
        var prefix = projectionNamespace + "_";

        foreach (var entityType in modelBuilder.Model.GetEntityTypes())
        {
            var tableName = entityType.GetTableName();

            if (string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            entityType.SetSchema(null);

            if (tableName.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            entityType.SetTableName(prefix + tableName);
        }
    }
}
