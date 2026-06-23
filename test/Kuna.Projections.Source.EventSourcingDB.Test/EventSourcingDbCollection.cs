using Xunit;

namespace Kuna.Projections.Source.EventSourcingDB.Test;

[CollectionDefinition(Name)]
public class EventSourcingDbCollection : ICollectionFixture<EventSourcingDbContainerFixture>
{
    public const string Name = "EventSourcingDb";
}
