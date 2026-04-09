using AutoFixture;
using FakeItEasy;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;
using Kuna.Projections.Core.Test.Shared.Projections;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Core.Test.ProjectionFactoryTests;

public class CreateFromModelTests
{
    [Fact]
    public void Should_Create_Projection_From_Existing_Model_And_Preserve_IsNew()
    {
        var modelId = Guid.NewGuid();
        var fixture = new Fixture();
        var model = fixture.Build<ItemModel>()
                           .With(x => x.Id, modelId)
                           .With(x => x.Name, "restored")
                           .Create();
        var stateStore = A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict());

        var factory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var projection = factory.CreateFromModel(model, isNew: false);

        projection.ShouldNotBeNull();
        projection.IsNew.ShouldBeFalse();
        projection.ModelState.ShouldBeSameAs(model);
    }

    [Fact]
    public void Should_Create_Projection_From_New_Model_When_IsNew_Is_True()
    {
        var modelId = Guid.NewGuid();
        var fixture = new Fixture();
        var model = fixture.Build<ItemModel>()
                           .With(x => x.Id, modelId)
                           .Create();
        var stateStore = A.Fake<IModelStateStore<ItemModel>>(opt => opt.Strict());

        var factory = new ProjectionFactory<ItemModel>(
            id => new ItemProjection(id),
            stateStore);

        var projection = factory.CreateFromModel(model, isNew: true);

        projection.ShouldNotBeNull();
        projection.IsNew.ShouldBeTrue();
        projection.ModelState.ShouldBeSameAs(model);
    }
}
