using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Pipeline.EF.Test.Items;

public class TestProjectionDbContext : SqlProjectionsDbContext
{
    private const string ProjectionSchema = "dbo";

    public TestProjectionDbContext(DbContextOptions options)
        : base(options, ProjectionSchema)
    {
    }

    public DbSet<TestModel> TestModels { get; set; }

    public DbSet<TestChildModel> TestChildModels { get; set; }

    public DbSet<TestChildItem> TestChildItems { get; set; }

    public DbSet<InvalidChildModel> InvalidChildModels { get; set; }

    public DbSet<InvalidChildItem> InvalidChildItems { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<TestModel>()
                    .Property(x => x.Name)
                    .HasMaxLength(32);

        modelBuilder.Entity<TestChildModel>()
                    .Property(x => x.Name)
                    .HasMaxLength(32);

        modelBuilder.Entity<TestChildModel>()
                    .HasMany(x => x.Children)
                    .WithOne()
                    .HasForeignKey(x => x.TestChildModelId);

        modelBuilder.Entity<TestChildItem>()
                    .Property(x => x.Value)
                    .HasMaxLength(32);

        modelBuilder.Entity<InvalidChildModel>()
                    .Property(x => x.Name)
                    .HasMaxLength(32);

        modelBuilder.Entity<InvalidChildModel>()
                    .HasMany(x => x.Children)
                    .WithOne()
                    .HasForeignKey(x => x.InvalidChildModelId);

        modelBuilder.Entity<InvalidChildItem>()
                    .Property(x => x.Value)
                    .HasMaxLength(32);

        base.OnModelCreating(modelBuilder);
    }
}
