using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Abstractions.Services;

public interface ICheckpointSerializer<TCheckpoint>
{
    GlobalEventPosition Serialize(TCheckpoint checkpoint);

    TCheckpoint Deserialize(GlobalEventPosition checkpoint);
}
