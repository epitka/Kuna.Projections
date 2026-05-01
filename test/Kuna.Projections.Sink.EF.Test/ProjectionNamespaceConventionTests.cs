using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF.Data;
using Microsoft.EntityFrameworkCore;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test;

public class ProjectionNamespaceConventionTests
{
    [Fact]
    public void SchemaCapableProvider_Should_Apply_Namespace_As_Schema()
    {
        using var dbContext = CreateSchemaDbContext();

        dbContext.Model.GetDefaultSchema().ShouldBe("orders");
        dbContext.Model.FindEntityType(typeof(TestModel))!.GetSchema().ShouldBe("orders");
        dbContext.Model.FindEntityType(typeof(TestModel))!.GetTableName().ShouldBe("TestModels");
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetSchema().ShouldBe("orders");
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetTableName().ShouldBe("CheckPoints");
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetSchema().ShouldBe("orders");
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetTableName().ShouldBe("ProjectionFailures");
    }

    [Fact]
    public void MySqlProvider_Should_Apply_Namespace_As_Table_Prefix()
    {
        using var dbContext = CreateMySqlDbContext();

        dbContext.Model.FindEntityType(typeof(TestModel))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(TestModel))!.GetTableName().ShouldBe("orders_TestModels");
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetTableName().ShouldBe("orders_CheckPoints");
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetTableName().ShouldBe("orders_ProjectionFailures");
    }

    [Fact]
    public void MissingNamespace_Should_Leave_Default_Table_Names_Intact()
    {
        using var dbContext = CreateUnqualifiedDbContext();

        dbContext.Model.FindEntityType(typeof(TestModel))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(TestModel))!.GetTableName().ShouldBe("TestModels");
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(CheckPoint))!.GetTableName().ShouldBe("CheckPoints");
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetSchema().ShouldBeNull();
        dbContext.Model.FindEntityType(typeof(ProjectionFailure))!.GetTableName().ShouldBe("ProjectionFailures");
    }

    private static SchemaAwareProjectionDbContext CreateSchemaDbContext()
    {
        return new SchemaAwareProjectionDbContext(CreateOptions<SchemaAwareProjectionDbContext>());
    }

    private static MySqlAwareProjectionDbContext CreateMySqlDbContext()
    {
        return new MySqlAwareProjectionDbContext(CreateOptions<MySqlAwareProjectionDbContext>());
    }

    private static UnqualifiedProjectionDbContext CreateUnqualifiedDbContext()
    {
        return new UnqualifiedProjectionDbContext(CreateOptions<UnqualifiedProjectionDbContext>());
    }

    private static DbContextOptions<TContext> CreateOptions<TContext>()
        where TContext : DbContext
    {
        return new DbContextOptionsBuilder<TContext>()
               .UseNpgsql("Host=localhost;Database=test;Username=test;Password=test")
               .Options;
    }

    private sealed class SchemaAwareProjectionDbContext : SqlProjectionsDbContext
    {
        public SchemaAwareProjectionDbContext(DbContextOptions<SchemaAwareProjectionDbContext> options)
            : base(options, "orders")
        {
        }

        public DbSet<TestModel> TestModels { get; set; }
    }

    private sealed class MySqlAwareProjectionDbContext : SqlProjectionsDbContext
    {
        public MySqlAwareProjectionDbContext(DbContextOptions<MySqlAwareProjectionDbContext> options)
            : base(options, "orders")
        {
        }

        public DbSet<TestModel> TestModels { get; set; }

        protected override string? GetProviderName()
        {
            return "Pomelo.EntityFrameworkCore.MySql";
        }
    }

    private sealed class UnqualifiedProjectionDbContext : SqlProjectionsDbContext
    {
        public UnqualifiedProjectionDbContext(DbContextOptions<UnqualifiedProjectionDbContext> options)
            : base(options, null)
        {
        }

        public DbSet<TestModel> TestModels { get; set; }

        protected override string? GetProviderName()
        {
            return "Pomelo.EntityFrameworkCore.MySql";
        }
    }
}
