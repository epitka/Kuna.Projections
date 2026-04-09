using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;
using Kuna.Projections.Abstractions.Services;
using Kuna.Projections.Core.Test.Shared.Models;

namespace Kuna.Projections.Core.Test.ModelStateBatcherTests;

internal static class Helpers
{
    internal static async Task<IReadOnlyList<ModelStatesBatch<ItemModel>>> RunBatcher(
        IProjectionSettings<ItemModel> settings,
        IEnumerable<ModelState<ItemModel>> changes)
    {
        using var system = ActorSystem.Create("projection-batcher-test");
        var materializer = ActorMaterializer.Create(system);

        var result = await Source.From(changes)
                                 .Via(ModelStateBatcher.Create<ItemModel>(settings))
                                 .RunWith(Sink.Seq<ModelStatesBatch<ItemModel>>(), materializer);

        materializer.Shutdown();
        await system.Terminate();

        return result;
    }

    internal static ModelState<ItemModel> CreateChange(Guid id, ulong position, string name = "")
    {
        return new ModelState<ItemModel>(
            new ItemModel
            {
                Id = id,
                Name = name,
                EventNumber = 1,
                GlobalEventPosition = new GlobalEventPosition(position),
            },
            IsNew: false,
            ShouldDelete: false,
            GlobalEventPosition: new GlobalEventPosition(position),
            ExpectedEventNumber: null);
    }
}
