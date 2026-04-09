using Kuna.Projections.Abstractions.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Kuna.Projections.Sink.EF.Data;

/// <summary>
/// EF Core mapping for persisted projection-processing failures.
/// </summary>
public class ProjectionFailureConfiguration : IEntityTypeConfiguration<ProjectionFailure>
{
    private readonly string schema;

    public ProjectionFailureConfiguration(string schema)
    {
        this.schema = string.IsNullOrWhiteSpace(schema)
            ? throw new ArgumentException("Projection schema must be provided.", nameof(schema))
            : schema;
    }

    /// <summary>
    /// Configures the projection-failure table schema and global-position
    /// conversion.
    /// </summary>
    public void Configure(EntityTypeBuilder<ProjectionFailure> builder)
    {
        const string tableName = "ProjectionFailures";
        builder.ToTable(tableName, this.schema);

        builder.HasKey(nameof(ProjectionFailure.ModelName), nameof(ProjectionFailure.ModelId));
        builder.Property(x => x.ModelId).IsRequired();
        builder.Property(x => x.ModelName).IsRequired();
        builder.Property(x => x.EventNumber).IsRequired();
        builder.Property(x => x.Exception).IsRequired();
        builder.Property(x => x.FailureCreatedOn).IsRequired();
        builder.Property(x => x.FailureType).IsRequired();
        builder.Property(x => x.GlobalEventPosition)
               .HasConversion(
                   value => (long)value.Value,
                   value => new GlobalEventPosition((ulong)value))
               .HasColumnType("bigint");
    }
}
