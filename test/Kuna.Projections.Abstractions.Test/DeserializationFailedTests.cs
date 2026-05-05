using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test;

public class DeserializationFailedTests
{
    [Fact]
    public void Should_Preserve_Metadata_Properties()
    {
        var createdOn = DateTime.UtcNow;
        var modelId = Guid.NewGuid();
        var evt = new DeserializationFailed
        {
            TypeName = nameof(DeserializationFailed),
            CreatedOn = createdOn,
            EventNumber = 7,
            ModelId = modelId,
            GlobalEventPosition = new GlobalEventPosition("99"),
        };

        evt.TypeName.ShouldBe(nameof(DeserializationFailed));
        evt.CreatedOn.ShouldBe(createdOn);
        evt.EventNumber.ShouldBe(7);
        evt.ModelId.ShouldBe(modelId);
        evt.GlobalEventPosition.ShouldBe(new GlobalEventPosition("99"));
    }
}
