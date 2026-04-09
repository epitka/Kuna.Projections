using Kuna.Projections.Abstractions.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Kuna.Projections.Sink.EF.Data;

/// <summary>
/// EF Core mapping for the projection checkpoint entity.
/// </summary>
public class CheckPointConfiguration : IEntityTypeConfiguration<CheckPoint>
{
    private readonly string schema;

    public CheckPointConfiguration(string schema)
    {
        this.schema = string.IsNullOrWhiteSpace(schema)
            ? throw new ArgumentException("Projection schema must be provided.", nameof(schema))
            : schema;
    }

    /// <summary>
    /// Configures the checkpoint table schema and global-position conversion.
    /// </summary>
    public void Configure(EntityTypeBuilder<CheckPoint> builder)
    {
        const string tableName = "CheckPoints";
        builder.ToTable(tableName, this.schema);

        builder.HasKey(nameof(CheckPoint.ModelName));

        builder.Property(x => x.ModelName)
               .IsRequired()
               .HasMaxLength(100);

        builder.Property(x => x.GlobalEventPosition)
               .HasConversion(
                   value => (long)value.Value,
                   value => new GlobalEventPosition((ulong)value))
               .HasColumnType("bigint");
    }
}
