using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Pipeline.EF.Test.Items;
using Kuna.Projections.Sink.EF;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Pipeline.EF.Test.DataStoreTests;

[Collection(PostgresSqlCollection.Name)]
public class PersistBatchTests : DataStoreIntegrationTestBase
{
    public PersistBatchTests(PostgresSqlContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task NewModel_Should_Be_Inserted_Without_Persisting_Checkpoint()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var modelId = Guid.NewGuid();
        var model = new TestModel
        {
            Id = modelId,
            Name = "created",
            EventNumber = 1,
            GlobalEventPosition = new GlobalEventPosition(11),
            HasStreamProcessingFaulted = false,
        };

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(
                    model,
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(11),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition(99),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedModel = await dbContext.TestModels.FindAsync(new object[] { modelId, }, CancellationToken.None);
        persistedModel.ShouldNotBeNull();
        persistedModel.Name.ShouldBe("created");
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task ExistingModel_Should_Be_Updated()
    {
        var modelId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        await SeedModel(provider, modelId, "before", 1, 5);
        var store = CreateStore(provider);
        var model = await store.Load(modelId, CancellationToken.None);

        model.ShouldNotBeNull();
        model.Name = "after";
        model.EventNumber = 2;
        model.GlobalEventPosition = new GlobalEventPosition(22);

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(
                    model,
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(22),
                    ExpectedEventNumber: 1),
            ],
            GlobalEventPosition = new GlobalEventPosition(120),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedModel = await dbContext.TestModels.FindAsync(new object[] { modelId, }, CancellationToken.None);
        persistedModel.ShouldNotBeNull();
        persistedModel.Name.ShouldBe("after");
        persistedModel.EventNumber.ShouldBe(2);
        persistedModel.GlobalEventPosition.ShouldBe(new GlobalEventPosition(22));
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task ExistingHierarchicalModel_WithNewChild_Should_Insert_Child_Row_On_Update()
    {
        var modelId = Guid.NewGuid();
        var childId = Guid.NewGuid();

        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);

        await using (var seedScope = provider.CreateAsyncScope())
        {
            var dbContext = seedScope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
            dbContext.TestChildModels.Add(
                new TestChildModel
                {
                    Id = modelId,
                    Name = "before",
                    EventNumber = 1,
                    GlobalEventPosition = new GlobalEventPosition(5),
                    HasStreamProcessingFaulted = false,
                });

            await dbContext.SaveChangesAsync(CancellationToken.None);
        }

        var duplicateKeyExceptionDetector = provider.GetRequiredService<IDuplicateKeyExceptionDetector>();
        var failureLogger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<TestChildModel, TestProjectionDbContext>>>();
        var failureHandler = new ProjectionFailureHandler<TestChildModel, TestProjectionDbContext>(provider, duplicateKeyExceptionDetector, failureLogger);
        var storeLogger = provider.GetRequiredService<ILogger<DataStore<TestChildModel, TestProjectionDbContext>>>();
        var store = new DataStore<TestChildModel, TestProjectionDbContext>(provider, duplicateKeyExceptionDetector, failureHandler, storeLogger);

        var model = await store.Load(modelId, CancellationToken.None);

        model.ShouldNotBeNull();
        model.Name = "after";
        model.EventNumber = 2;
        model.GlobalEventPosition = new GlobalEventPosition(22);
        model.Children.Add(
            new TestChildItem
            {
                Id = childId,
                TestChildModelId = modelId,
                Value = "new-child",
            });

        var batch = new ModelStatesBatch<TestChildModel>
        {
            Changes =
            [
                new ModelState<TestChildModel>(
                    model,
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(22),
                    ExpectedEventNumber: 1),
            ],
            GlobalEventPosition = new GlobalEventPosition(220),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        await using var verifyScope = provider.CreateAsyncScope();
        var verifyDbContext = verifyScope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedModel = await verifyDbContext.TestChildModels
                                                  .Include(x => x.Children)
                                                  .SingleAsync(x => x.Id == modelId, CancellationToken.None);

        persistedModel.Name.ShouldBe("after");
        persistedModel.Children.Count.ShouldBe(1);
        persistedModel.Children[0].Id.ShouldBe(childId);
        persistedModel.Children[0].Value.ShouldBe("new-child");
    }

    [Fact]
    public void HierarchicalModel_Without_PersistedChildEntity_Should_Fail_Fast()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var duplicateKeyExceptionDetector = provider.GetRequiredService<IDuplicateKeyExceptionDetector>();
        var failureLogger = provider.GetRequiredService<ILogger<ProjectionFailureHandler<InvalidChildModel, TestProjectionDbContext>>>();
        var failureHandler = new ProjectionFailureHandler<InvalidChildModel, TestProjectionDbContext>(provider, duplicateKeyExceptionDetector, failureLogger);
        var storeLogger = provider.GetRequiredService<ILogger<DataStore<InvalidChildModel, TestProjectionDbContext>>>();

        var ex = Should.Throw<InvalidOperationException>(
            () => new DataStore<InvalidChildModel, TestProjectionDbContext>(provider, duplicateKeyExceptionDetector, failureHandler, storeLogger));

        ex.Message.ShouldContain($"{nameof(InvalidChildModel)}.{nameof(InvalidChildModel.Children)}");
        ex.Message.ShouldContain(nameof(InvalidChildItem));
        ex.Message.ShouldContain(nameof(ChildEntity));
    }

    [Fact]
    public async Task ExistingModelMarkedForDelete_Should_Be_Removed()
    {
        var modelId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        await SeedModel(provider, modelId, "to-delete", 4, 40);
        var store = CreateStore(provider);
        var model = await store.Load(modelId, CancellationToken.None);

        model.ShouldNotBeNull();

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(
                    model,
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition(40),
                    ExpectedEventNumber: 4),
            ],
            GlobalEventPosition = new GlobalEventPosition(140),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedModel = await dbContext.TestModels.FindAsync(new object[] { modelId, }, CancellationToken.None);
        persistedModel.ShouldBeNull();
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task InvalidNewAndDeleteChange_Should_Be_Skipped_Without_Persisting_Checkpoint()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var modelId = Guid.NewGuid();

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "invalid",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition(1),
                        HasStreamProcessingFaulted = false,
                    },
                    IsNew: true,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition(1),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition(200),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedModel = await dbContext.TestModels.FindAsync(new object[] { modelId, }, CancellationToken.None);
        persistedModel.ShouldBeNull();
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task InvalidInsertInSmallBatch_Should_Record_Failure_And_Still_Persist_Valid_Model()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var validId = Guid.NewGuid();
        var invalidId = Guid.NewGuid();

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = validId,
                        Name = "valid",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition(10),
                        HasStreamProcessingFaulted = false,
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(10),
                    ExpectedEventNumber: null),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = invalidId,
                        Name = new string('x', 128),
                        EventNumber = 2,
                        GlobalEventPosition = new GlobalEventPosition(11),
                        HasStreamProcessingFaulted = false,
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition(11),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition(201),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();

        var validModel = await dbContext.TestModels.FindAsync(new object[] { validId, }, CancellationToken.None);
        var invalidModel = await dbContext.TestModels.FindAsync(new object[] { invalidId, }, CancellationToken.None);
        var failure = await dbContext.ProjectionFailures.FindAsync(
                          new object[] { ProjectionModelName.For<TestModel>(), invalidId, },
                          CancellationToken.None);

        validModel.ShouldNotBeNull();
        invalidModel.ShouldBeNull();
        failure.ShouldNotBeNull();
        failure.FailureType.ShouldBe(nameof(FailureType.Persistence));
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task MultipleInvalidInsertsInLargeBatch_Should_Be_Isolated_Recursively()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var invalidId1 = Guid.NewGuid();
        var invalidId2 = Guid.NewGuid();
        var changes = new ModelState<TestModel>[22];

        for (var i = 0; i < changes.Length; i++)
        {
            var id = Guid.NewGuid();
            var name = $"m-{i:D2}";

            if (i == 7)
            {
                id = invalidId1;
                name = new string('a', 120);
            }
            else if (i == 18)
            {
                id = invalidId2;
                name = new string('b', 140);
            }

            changes[i] = new ModelState<TestModel>(
                new TestModel
                {
                    Id = id,
                    Name = name,
                    EventNumber = i + 1,
                    GlobalEventPosition = new GlobalEventPosition((ulong)(1000 + i)),
                    HasStreamProcessingFaulted = false,
                },
                IsNew: true,
                ShouldDelete: false,
                GlobalEventPosition: new GlobalEventPosition((ulong)(1000 + i)),
                ExpectedEventNumber: null);
        }

        await store.PersistBatch(
            new ModelStatesBatch<TestModel>
            {
                Changes = changes,
                GlobalEventPosition = new GlobalEventPosition(500),
            },
            CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();

        var modelCount = await dbContext.TestModels.CountAsync(CancellationToken.None);
        var failures = await dbContext.ProjectionFailures.ToListAsync(CancellationToken.None);
        var invalid1 = await dbContext.TestModels.FindAsync(new object[] { invalidId1, }, CancellationToken.None);
        var invalid2 = await dbContext.TestModels.FindAsync(new object[] { invalidId2, }, CancellationToken.None);
        modelCount.ShouldBe(20);
        invalid1.ShouldBeNull();
        invalid2.ShouldBeNull();
        failures.Count.ShouldBe(2);
        failures.ShouldContain(x => x.ModelId == invalidId1 && x.FailureType == nameof(FailureType.Persistence));
        failures.ShouldContain(x => x.ModelId == invalidId2 && x.FailureType == nameof(FailureType.Persistence));
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task ManyInvalidInsertsInVeryLargeBatch_Should_Be_Isolated_Recursively()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);
        var invalidIds = new[]
        {
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.NewGuid(),
        };

        var invalidIndexes = new[] { 3, 17, 34, 49, };
        var changes = new ModelState<TestModel>[64];

        for (var i = 0; i < changes.Length; i++)
        {
            var id = Guid.NewGuid();
            var name = $"ok-{i:D2}";

            for (var j = 0; j < invalidIndexes.Length; j++)
            {
                if (i != invalidIndexes[j])
                {
                    continue;
                }

                id = invalidIds[j];
                name = new string((char)('m' + j), 200);
                break;
            }

            changes[i] = new ModelState<TestModel>(
                new TestModel
                {
                    Id = id,
                    Name = name,
                    EventNumber = i + 1,
                    GlobalEventPosition = new GlobalEventPosition((ulong)(2000 + i)),
                    HasStreamProcessingFaulted = false,
                },
                IsNew: true,
                ShouldDelete: false,
                GlobalEventPosition: new GlobalEventPosition((ulong)(2000 + i)),
                ExpectedEventNumber: null);
        }

        await store.PersistBatch(
            new ModelStatesBatch<TestModel>
            {
                Changes = changes,
                GlobalEventPosition = new GlobalEventPosition(777),
            },
            CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var modelCount = await dbContext.TestModels.CountAsync(CancellationToken.None);
        var failures = await dbContext.ProjectionFailures
                                      .Where(x => x.ModelName == ProjectionModelName.For<TestModel>())
                                      .ToListAsync(CancellationToken.None);

        modelCount.ShouldBe(60);
        failures.Count.ShouldBe(4);

        foreach (var invalidId in invalidIds)
        {
            var invalidModel = await dbContext.TestModels.FindAsync(new object[] { invalidId, }, CancellationToken.None);
            invalidModel.ShouldBeNull();
            failures.ShouldContain(x => x.ModelId == invalidId && x.FailureType == nameof(FailureType.Persistence));
        }

        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task FailingUpdate_Should_Fall_Back_To_Isolated_Updates_And_Record_Failure()
    {
        var validId = Guid.NewGuid();
        var invalidId = Guid.NewGuid();
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        await SeedModel(provider, validId, "before-valid", 1, 1);
        await SeedModel(provider, invalidId, "before-invalid", 1, 2);
        var store = CreateStore(provider);
        var validModel = await store.Load(validId, CancellationToken.None);
        var invalidModel = await store.Load(invalidId, CancellationToken.None);

        validModel.ShouldNotBeNull();
        invalidModel.ShouldNotBeNull();

        validModel.Name = "after-valid";
        validModel.EventNumber = 2;
        validModel.GlobalEventPosition = new GlobalEventPosition(101);

        invalidModel.Name = new string('y', 256);
        invalidModel.EventNumber = 2;
        invalidModel.GlobalEventPosition = new GlobalEventPosition(102);

        var batch = new ModelStatesBatch<TestModel>
        {
            Changes =
            [
                new ModelState<TestModel>(validModel, false, false, validModel.GlobalEventPosition, 1),
                new ModelState<TestModel>(invalidModel, false, false, invalidModel.GlobalEventPosition, 1),
            ],
            GlobalEventPosition = new GlobalEventPosition(300),
        };

        await store.PersistBatch(batch, CancellationToken.None);

        using var verifyScope = provider.CreateScope();
        await using var dbContext = verifyScope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var persistedValid = await dbContext.TestModels.FindAsync(new object[] { validId, }, CancellationToken.None);
        var persistedInvalid = await dbContext.TestModels.FindAsync(new object[] { invalidId, }, CancellationToken.None);
        var failure = await dbContext.ProjectionFailures.FindAsync(
                          new object[] { ProjectionModelName.For<TestModel>(), invalidId, },
                          CancellationToken.None);

        persistedValid.ShouldNotBeNull();
        persistedValid.Name.ShouldBe("after-valid");
        persistedValid.EventNumber.ShouldBe(2);
        persistedValid.GlobalEventPosition.ShouldBe(new GlobalEventPosition(101));

        persistedInvalid.ShouldNotBeNull();
        persistedInvalid.Name.ShouldBe("before-invalid");
        persistedInvalid.HasStreamProcessingFaulted.ShouldBeTrue();

        failure.ShouldNotBeNull();
        failure.FailureType.ShouldBe(nameof(FailureType.Persistence));
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }

    [Fact]
    public async Task EmptyChanges_Should_Not_Persist_Models_Or_Checkpoint()
    {
        using var provider = PostgresSqlTestHelper.CreateServiceProvider(this.Fixture);
        var store = CreateStore(provider);

        await store.PersistBatch(
            new ModelStatesBatch<TestModel>
            {
                Changes = Array.Empty<ModelState<TestModel>>(),
                GlobalEventPosition = new GlobalEventPosition(400),
            },
            CancellationToken.None);

        using var scope = provider.CreateScope();
        await using var dbContext = scope.ServiceProvider.GetRequiredService<TestProjectionDbContext>();
        var modelCount = await dbContext.TestModels.CountAsync(CancellationToken.None);
        var failureCount = await dbContext.ProjectionFailures.CountAsync(CancellationToken.None);
        modelCount.ShouldBe(0);
        failureCount.ShouldBe(0);
        (await dbContext.CheckPoint.FindAsync(new object[] { ProjectionModelName.For<TestModel>(), }, CancellationToken.None))
            .ShouldBeNull();
    }
}
