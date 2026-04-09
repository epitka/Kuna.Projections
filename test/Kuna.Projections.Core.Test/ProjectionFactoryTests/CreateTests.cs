using AutoFixture;
using FakeItEasy;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Kuna.Projections.Core.Test.Shared.Projections;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionFactoryTests;

public class CreateTests
{
    [Fact]
    public async Task When_Load_Not_Requested_Should_Create_New_Projection()
    {
        var modelId = Guid.NewGuid();
        var stateStore = A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict());

        var factory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var projection = await factory.Create(modelId, loadModelFromStore: false, CancellationToken.None);

        projection.ShouldNotBeNull();
        projection!.IsNew.ShouldBeTrue();
        projection.ModelState.Id.ShouldBe(modelId);
    }

    [Fact]
    public async Task When_Model_Found_Should_Load_Existing_State()
    {
        var modelId = Guid.NewGuid();
        var fixture = new Fixture();
        var model = fixture.Build<ItemModel>()
                           .With(x => x.Id, modelId)
                           .Create();

        var stateStore = A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict());
        A.CallTo(() => stateStore.Load(modelId, A<CancellationToken>._)).Returns(model);

        var factory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var projection = await factory.Create(modelId, loadModelFromStore: true, CancellationToken.None);

        projection.ShouldNotBeNull();
        projection!.IsNew.ShouldBeFalse();
        projection.ModelState.ShouldBeSameAs(model);
    }

    [Fact]
    public async Task When_Model_Not_Found_Should_Return_Null()
    {
        var modelId = Guid.NewGuid();
        var stateStore = A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict());
        A.CallTo(() => stateStore.Load(modelId, A<CancellationToken>._)).Returns((ItemModel?)null);

        var factory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var projection = await factory.Create(modelId, loadModelFromStore: true, CancellationToken.None);

        projection.ShouldBeNull();
    }
}
