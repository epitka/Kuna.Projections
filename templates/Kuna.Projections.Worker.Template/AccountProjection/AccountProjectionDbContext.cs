using Kuna.Projections.Sink.EF.Data;
using Kuna.Projections.Worker.Template.AccountProjection.Model;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Worker.Template.AccountProjection;

public sealed class AccountProjectionDbContext : SqlProjectionsDbContext
{
    public AccountProjectionDbContext(
        DbContextOptions<AccountProjectionDbContext> options,
        ProjectionSchema<AccountProjectionDbContext> projectionSchema)
        : base(options, projectionSchema.Value)
    {
    }

    public DbSet<Account> Accounts { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Account>(
            account =>
            {
                account.Property(x => x.Email).HasMaxLength(250);
                account.HasIndex(x => x.Email);
            });

        base.OnModelCreating(modelBuilder);
    }
}
