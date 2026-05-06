using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Kuna.Projections.Sink.EF.Data;

/// <summary>
/// EF Core mapping for the projection checkpoint entity.
/// </summary>
public class CheckPointConfiguration : IEntityTypeConfiguration<CheckPoint>
{
    private readonly string? schema;

    public CheckPointConfiguration(string? schema)
    {
        this.schema = ProjectionNamespaceConvention.Normalize(schema);
    }

    /// <summary>
    /// Configures the checkpoint table schema and global-position conversion.
    /// </summary>
    public void Configure(EntityTypeBuilder<CheckPoint> builder)
    {
        const string tableName = "CheckPoints";

        if (this.schema == null)
        {
            builder.ToTable(tableName);
        }
        else
        {
            builder.ToTable(tableName, this.schema);
        }

        builder.HasKey(nameof(CheckPoint.ModelName), nameof(CheckPoint.InstanceId));

        builder.Property(x => x.ModelName)
               .IsRequired()
               .HasMaxLength(100);

        builder.Property(x => x.InstanceId)
               .IsRequired()
               .HasMaxLength(100);

        builder.Property(x => x.GlobalEventPosition)
               .HasConversion(
                   value => value.Value,
                   value => new GlobalEventPosition(value))
               .HasMaxLength(128);
    }
}
