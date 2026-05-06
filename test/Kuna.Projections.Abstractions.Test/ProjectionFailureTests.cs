using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test;

public class ProjectionFailureTests
{
    [Fact]
    public void Constructor_Should_Set_All_Properties()
    {
        var modelId = Guid.NewGuid();
        var position = new GlobalEventPosition("55");
        var failureTime = DateTime.UtcNow;

        var failure = new ProjectionFailure(
            modelId: modelId,
            eventNumber: 12,
            streamPosition: position,
            failureCreatedOn: failureTime,
            exception: "boom",
            failureType: "Test",
            modelName: "TestModel",
            instanceId: "orders-v1");

        failure.ModelId.ShouldBe(modelId);
        failure.EventNumber.ShouldBe(12);
        failure.GlobalEventPosition.ShouldBe(position);
        failure.FailureCreatedOn.ShouldBe(failureTime);
        failure.Exception.ShouldBe("boom");
        failure.FailureType.ShouldBe("Test");
        failure.ModelName.ShouldBe("TestModel");
        failure.InstanceId.ShouldBe("orders-v1");
    }
}
