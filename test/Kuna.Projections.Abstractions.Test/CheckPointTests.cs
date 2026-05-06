using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test;

public class CheckPointTests
{
    [Fact]
    public void Should_Preserve_Properties()
    {
        var checkPoint = new CheckPoint
        {
            ModelName = "Orders",
            InstanceId = "orders-v1",
            GlobalEventPosition = new GlobalEventPosition("42"),
        };

        checkPoint.ModelName.ShouldBe("Orders");
        checkPoint.InstanceId.ShouldBe("orders-v1");
        checkPoint.GlobalEventPosition.ShouldBe(new GlobalEventPosition("42"));
    }
}
