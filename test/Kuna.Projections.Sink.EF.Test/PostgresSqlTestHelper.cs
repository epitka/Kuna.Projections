using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.EF.Npgsql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Kuna.Projections.Pipeline.EF.Test;

public static class PostgresSqlTestHelper
{
    private static readonly object DatabaseResetLock = new();

    public static void ResetDatabase(PostgresSqlContainerFixture fixture)
    {
        lock (DatabaseResetLock)
        {
            using var dbContext = CreateDbContext(fixture);
            dbContext.Database.EnsureDeleted();
            dbContext.Database.EnsureCreated();
        }
    }

    public static ServiceProvider CreateServiceProvider(PostgresSqlContainerFixture fixture)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddNpgsqlDuplicateKeyDetection();
        services.AddDbContext<TestProjectionDbContext>(
            options => Configure(options, fixture.ConnectionString),
            ServiceLifetime.Scoped);

        services.AddSingleton<IProjectionSettings<TestModel>>(new ProjectionSettings<TestModel> { InstanceId = "test-model", });
        services.AddSingleton<IProjectionSettings<TestChildModel>>(new ProjectionSettings<TestChildModel> { InstanceId = "test-child-model", });
        services.AddSingleton<IProjectionSettings<InvalidChildModel>>(new ProjectionSettings<InvalidChildModel> { InstanceId = "invalid-child-model", });

        return services.BuildServiceProvider();
    }

    public static TestProjectionDbContext CreateDbContext(PostgresSqlContainerFixture fixture)
    {
        var options = new DbContextOptionsBuilder<TestProjectionDbContext>();
        Configure(options, fixture.ConnectionString);
        return new TestProjectionDbContext(options.Options);
    }

    private static void Configure(DbContextOptionsBuilder options, string connectionString)
    {
        AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

        options.UseNpgsql(
                   connectionString,
                   npgsqlOptions =>
                   {
                       npgsqlOptions.EnableRetryOnFailure(
                           3,
                           TimeSpan.FromMilliseconds(250),
                           null);
                   })
               .EnableSensitiveDataLogging();
    }
}
