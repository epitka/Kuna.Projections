using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Sink.MongoDB.Test.Items;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Sink.MongoDB.Test;

[Collection(MongoDbCollection.Name)]
public sealed class PersistBatchTests : MongoDbIntegrationTestBase
{
    public PersistBatchTests(MongoDbContainerFixture fixture)
        : base(fixture)
    {
    }

    [Fact]
    public async Task PersistBatch_Should_Insert_New_Model()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "created",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition("10"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("10"),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition("10"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("created");
        document["EventNumber"].AsInt64.ShouldBe(1);
        document["GlobalEventPosition"].AsString.ShouldBe("10");
    }

    [Fact]
    public async Task PersistBatch_Should_Update_Model_When_Expected_Event_Number_Matches()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "before", 1, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "after",
                        EventNumber = 2,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 1),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("after");
        document["EventNumber"].AsInt64.ShouldBe(2);
        document["GlobalEventPosition"].AsString.ShouldBe("11");
    }

    [Fact]
    public async Task PersistBatch_Should_Delete_Model_When_Expected_Event_Number_Matches()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "to-delete", 2, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "to-delete",
                        EventNumber = 3,
                        GlobalEventPosition = new GlobalEventPosition("12"),
                    },
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition("12"),
                    ExpectedEventNumber: 2),
            ],
            GlobalEventPosition = new GlobalEventPosition("12"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldBeNull();
    }

    [Fact]
    public async Task PersistBatch_Should_Skip_Duplicate_Insert()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "existing", 1, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "duplicate",
                        EventNumber = 2,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);
        var count = await this.GetModelDocumentCount(provider, modelId);

        count.ShouldBe(1);
        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("existing");
        document["EventNumber"].AsInt64.ShouldBe(1);
    }

    [Fact]
    public async Task PersistBatch_Should_Record_Failure_For_Stale_Update()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "before", 3, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "after",
                        EventNumber = 4,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 2),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);
        var failureDocument = await this.GetFailureDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("before");
        document["EventNumber"].AsInt64.ShouldBe(3);
        document["HasStreamProcessingFaulted"].AsBoolean.ShouldBeTrue();
        failureDocument.ShouldNotBeNull();
        failureDocument["EventNumber"].AsInt64.ShouldBe(4);
        failureDocument["InstanceId"].AsString.ShouldBe(SettingsSectionName);
        failureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.Persistence));
    }

    [Fact]
    public async Task PersistBatch_Should_Record_Failure_With_Configured_Instance_Id()
    {
        const string instanceId = "orders-v2";
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider(instanceId);
        await this.SeedModel(provider, modelId, "before", 3, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "after",
                        EventNumber = 4,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 2),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var failureDocument = await this.GetFailureDocument(provider, modelId, instanceId);

        failureDocument.ShouldNotBeNull();
        failureDocument["InstanceId"].AsString.ShouldBe(instanceId);
        failureDocument["EventNumber"].AsInt64.ShouldBe(4);
    }

    [Fact]
    public async Task PersistBatch_Should_Skip_Stale_Delete()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, modelId, "existing", 5, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "existing",
                        EventNumber = 6,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 4),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldNotBeNull();
        document["Name"].AsString.ShouldBe("existing");
        document["EventNumber"].AsInt64.ShouldBe(5);
    }

    [Fact]
    public async Task PersistBatch_Should_Skip_New_And_Deleted_Item_In_Same_Flush()
    {
        var modelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = modelId,
                        Name = "skip-me",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition("10"),
                    },
                    IsNew: true,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition("10"),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition("10"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var document = await this.GetModelDocument(provider, modelId);

        document.ShouldBeNull();
    }

    [Fact]
    public async Task PersistBatch_Should_Persist_Healthy_Insert_When_Sibling_Insert_Is_Duplicate()
    {
        var existingModelId = Guid.NewGuid();
        var newModelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, existingModelId, "existing", 1, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = existingModelId,
                        Name = "duplicate",
                        EventNumber = 2,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: null),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = newModelId,
                        Name = "new",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var existingDocument = await this.GetModelDocument(provider, existingModelId);
        var newDocument = await this.GetModelDocument(provider, newModelId);

        existingDocument.ShouldNotBeNull();
        existingDocument["Name"].AsString.ShouldBe("existing");
        newDocument.ShouldNotBeNull();
        newDocument["Name"].AsString.ShouldBe("new");
    }

    [Fact]
    public async Task PersistBatch_Should_Record_Failure_And_Persist_Healthy_Update_When_Sibling_Update_Is_Stale()
    {
        var staleModelId = Guid.NewGuid();
        var validModelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, staleModelId, "stale-before", 3, "10");
        await this.SeedModel(provider, validModelId, "valid-before", 5, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = staleModelId,
                        Name = "stale-after",
                        EventNumber = 4,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 2),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = validModelId,
                        Name = "valid-after",
                        EventNumber = 6,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 5),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var staleDocument = await this.GetModelDocument(provider, staleModelId);
        var validDocument = await this.GetModelDocument(provider, validModelId);
        var staleFailureDocument = await this.GetFailureDocument(provider, staleModelId);

        staleDocument.ShouldNotBeNull();
        staleDocument["Name"].AsString.ShouldBe("stale-before");
        staleDocument["HasStreamProcessingFaulted"].AsBoolean.ShouldBeTrue();
        validDocument.ShouldNotBeNull();
        validDocument["Name"].AsString.ShouldBe("valid-after");
        validDocument["EventNumber"].AsInt64.ShouldBe(6);
        staleFailureDocument.ShouldNotBeNull();
        staleFailureDocument["EventNumber"].AsInt64.ShouldBe(4);
        staleFailureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.Persistence));
    }

    [Fact]
    public async Task PersistBatch_Should_Persist_Healthy_Delete_When_Sibling_Delete_Is_Stale()
    {
        var staleModelId = Guid.NewGuid();
        var validModelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, staleModelId, "stale-existing", 3, "10");
        await this.SeedModel(provider, validModelId, "valid-existing", 5, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = staleModelId,
                        Name = "stale-existing",
                        EventNumber = 4,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 2),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = validModelId,
                        Name = "valid-existing",
                        EventNumber = 6,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: true,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 5),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var staleDocument = await this.GetModelDocument(provider, staleModelId);
        var validDocument = await this.GetModelDocument(provider, validModelId);

        staleDocument.ShouldNotBeNull();
        staleDocument["Name"].AsString.ShouldBe("stale-existing");
        validDocument.ShouldBeNull();
    }

    [Fact]
    public async Task PersistBatch_Should_Record_Failure_And_Persist_Healthy_Insert_When_Sibling_Insert_Is_Too_Large()
    {
        var failedModelId = Guid.NewGuid();
        var validModelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = failedModelId,
                        Name = new string('x', 17 * 1024 * 1024),
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: null),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = validModelId,
                        Name = "valid",
                        EventNumber = 1,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: true,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: null),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var failedDocument = await this.GetModelDocument(provider, failedModelId);
        var validDocument = await this.GetModelDocument(provider, validModelId);
        var failureDocument = await this.GetFailureDocument(provider, failedModelId);

        failedDocument.ShouldBeNull();
        validDocument.ShouldNotBeNull();
        validDocument["Name"].AsString.ShouldBe("valid");
        failureDocument.ShouldNotBeNull();
        failureDocument["ModelId"].AsString.ShouldBe(failedModelId.ToString("D"));
        failureDocument["EventNumber"].AsInt64.ShouldBe(1);
        failureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.Persistence));
    }

    [Fact]
    public async Task PersistBatch_Should_Record_Failure_And_Persist_Healthy_Update_When_Sibling_Update_Is_Too_Large()
    {
        var failedModelId = Guid.NewGuid();
        var validModelId = Guid.NewGuid();

        await using var provider = this.CreateProvider();
        await this.SeedModel(provider, failedModelId, "failed-before", 3, "10");
        await this.SeedModel(provider, validModelId, "valid-before", 5, "10");
        var sink = provider.GetRequiredKeyedService<IModelStateSink<TestModel>>(GetRegistrationKey<TestModel>());
        ModelStatesBatch<TestModel> batch = new()
        {
            Changes =
            [
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = failedModelId,
                        Name = new string('x', 17 * 1024 * 1024),
                        EventNumber = 4,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 3),
                new ModelState<TestModel>(
                    new TestModel
                    {
                        Id = validModelId,
                        Name = "valid-after",
                        EventNumber = 6,
                        GlobalEventPosition = new GlobalEventPosition("11"),
                    },
                    IsNew: false,
                    ShouldDelete: false,
                    GlobalEventPosition: new GlobalEventPosition("11"),
                    ExpectedEventNumber: 5),
            ],
            GlobalEventPosition = new GlobalEventPosition("11"),
        };

        await sink.PersistBatch(batch, CancellationToken.None);

        var failedDocument = await this.GetModelDocument(provider, failedModelId);
        var validDocument = await this.GetModelDocument(provider, validModelId);
        var failureDocument = await this.GetFailureDocument(provider, failedModelId);

        failedDocument.ShouldNotBeNull();
        failedDocument["Name"].AsString.ShouldBe("failed-before");
        failedDocument["EventNumber"].AsInt64.ShouldBe(3);
        failedDocument["HasStreamProcessingFaulted"].AsBoolean.ShouldBeTrue();
        validDocument.ShouldNotBeNull();
        validDocument["Name"].AsString.ShouldBe("valid-after");
        validDocument["EventNumber"].AsInt64.ShouldBe(6);
        failureDocument.ShouldNotBeNull();
        failureDocument["EventNumber"].AsInt64.ShouldBe(4);
        failureDocument["FailureType"].AsString.ShouldBe(nameof(FailureType.Persistence));
    }
}
