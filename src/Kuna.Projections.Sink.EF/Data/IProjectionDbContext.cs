using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Sink.EF;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace Kuna.Projections.Sink.EF.Data;

/// <summary>
/// Defines the EF entities required by the projection sink for checkpoint and
/// projection-failure persistence.
/// </summary>
public interface IProjectionDbContext
{
    DbSet<ProjectionFailure> ProjectionFailures { get; set; }

    DbSet<CheckPoint> CheckPoint { get; set; }
}

/// <summary>
/// Base EF Core DbContext implementation for the projection sink schema.
/// Application projects are expected to derive their concrete DbContext from
/// this type and add mapping for their own projection model entities alongside
/// the built-in checkpoint and projection-failure entities.
/// </summary>
public class SqlProjectionsDbContext
    : DbContext,
      IProjectionDbContext
{
    protected SqlProjectionsDbContext(DbContextOptions options, string? projectionSchema)
        : base(options)
    {
        this.ProjectionSchema = ProjectionNamespaceConvention.Normalize(projectionSchema);
    }

    public DbSet<ProjectionFailure> ProjectionFailures { get; set; }

    public DbSet<CheckPoint> CheckPoint { get; set; }

    /// <summary>
    /// Schema applied to both projection infrastructure tables and projection
    /// model tables.
    /// </summary>
    private string? ProjectionSchema { get; }

    protected virtual string? GetProviderName()
    {
        return this.Database.ProviderName;
    }

    /// <summary>
    /// Applies EF mappings for checkpoint and projection-failure entities.
    /// </summary>
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        var providerName = this.GetProviderName();
        var tableSchema = ProjectionNamespaceConvention.GetSchema(this.ProjectionSchema, providerName);

        modelBuilder.ApplyConfiguration(new ProjectionFailureConfiguration(tableSchema))
                    .ApplyConfiguration(new CheckPointConfiguration(tableSchema));

        foreach (var entityType in modelBuilder.Model.GetEntityTypes())
        {
            if (!typeof(Kuna.Projections.Abstractions.Models.IModel).IsAssignableFrom(entityType.ClrType))
            {
                continue;
            }

            ConfigureProjectionModel(modelBuilder, entityType);
        }

        ProjectionNamespaceConvention.Apply(modelBuilder, this.ProjectionSchema, providerName);
    }

    private static void ConfigureProjectionModel(ModelBuilder modelBuilder, IMutableEntityType entityType)
    {
        var entity = modelBuilder.Entity(entityType.ClrType);

        entity.HasKey(nameof(Kuna.Projections.Abstractions.Models.IModel.Id));
        entity.Property<long?>(nameof(Kuna.Projections.Abstractions.Models.IModel.EventNumber)).IsConcurrencyToken();
        entity.Property<GlobalEventPosition>(nameof(Kuna.Projections.Abstractions.Models.IModel.GlobalEventPosition))
              .HasConversion(
                  value => value.Value,
                  value => new GlobalEventPosition(value))
              .HasMaxLength(128);
    }
}
