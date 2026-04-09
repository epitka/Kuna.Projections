using Kuna.Projections.Abstractions.Messages;
using Shouldly;
using Xunit;

namespace Kuna.Projections.Abstractions.Test;

public class UnknownEventTests
{
    [Fact]
    public void UnknownEvent_Should_Preserve_Base_And_Unknown_Name_Properties()
    {
        var createdOn = DateTime.UtcNow;
        var unknown = new UnknownEvent
        {
            TypeName = nameof(UnknownEvent),
            UnknownEventName = "LegacyOrderCreated",
            CreatedOn = createdOn,
        };

        unknown.TypeName.ShouldBe(nameof(UnknownEvent));
        unknown.UnknownEventName.ShouldBe("LegacyOrderCreated");
        unknown.CreatedOn.ShouldBe(createdOn);
    }
}
