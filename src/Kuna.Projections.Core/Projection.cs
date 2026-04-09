#nullable disable
using Kuna.Projections.Abstractions.Exceptions;
using Kuna.Projections.Abstractions.Messages;
using Kuna.Projections.Abstractions.Models;

namespace Kuna.Projections.Core;

/// <summary>
/// Stateful runtime projection that owns a model instance and applies incoming
/// events to evolve that model in memory. The projection encapsulates event
/// handling behavior, tracks runtime flags such as whether the model is new or
/// should be deleted, and produces updated model state for downstream batching
/// and persistence.
/// </summary>
public abstract class Projection<TState>
    where TState : class, IModel, new()
{
    protected internal Projection(Guid modelId)
    {
        this.ModelState = new TState
        {
            Id = modelId,
            EventNumber = -1,
        };

        this.IsNew = true;
    }

    private Projection()
    {
    }

    /// <summary>
    /// Indicates whether this projection has been persisted before.
    /// </summary>
    public bool IsNew { get; set; }

    public bool ShouldDelete { get; protected internal set; }

    public TState ModelState { get; private set; }

    public TState My => this.ModelState;

    internal void SetModelState(TState model)
    {
        this.ModelState =  model;
    }

    /// <summary>
    /// Handles an event whose source type name could not be mapped to a
    /// registered CLR event type for this projection.
    /// Override to explicitly ignore or handle unsupported stream events.
    /// </summary>
    public virtual void Apply(UnknownEvent @event)
    {
        throw new Exception(
            $"Unknown event type encountered. Check your event definitions or override method if you want to ignore certain events. ModelState : {typeof(TState).Name}, model name: {@event.UnknownEventName}");
    }

    /// <summary>
    /// Applies the envelope's event to the current model state, enforcing the
    /// configured event version check before dispatch and updating the model's
    /// event metadata after a successful apply.
    /// Returns <see langword="false"/> when processing should be skipped for
    /// this envelope, for example because the model has already faulted or the
    /// event is stale under the selected version-check strategy.
    /// </summary>
    public bool Process(
        EventEnvelope msg,
        EventVersionCheckStrategy versionCheckStrategy = EventVersionCheckStrategy.Consecutive)
    {
        var proceed = this.PreProcess(msg, versionCheckStrategy);

        if (proceed == false)
        {
            return false;
        }

        try
        {
            ((dynamic)this).Apply((dynamic)msg.Event);
        }
        catch (Exception ex)
        {
            throw new Exception(
                $"Apply for {msg.Event.TypeName} for {typeof(TState).Name} modelId: {this.ModelState.Id} failed, see inner exception for details",
                ex);
        }

        this.PostProcess(msg);

        return true;
    }

    /// <summary>
    /// Handles an event whose payload could not be deserialized successfully.
    /// Override to tolerate specific deserialization failures instead of failing
    /// projection processing.
    /// </summary>
    protected virtual void Apply(DeserializationFailed @event)
    {
        throw new Exception(
            $"Deserialization of event failed. ModelState: {typeof(TState).Name}, modelId: {@event.ModelId}, event number: {@event.EventNumber}");
    }

    private bool PreProcess(EventEnvelope msg, EventVersionCheckStrategy versionCheckStrategy)
    {
        if (this.ModelState.HasStreamProcessingFaulted)
        {
            return false;
        }

        if (versionCheckStrategy == EventVersionCheckStrategy.Disabled)
        {
            return true;
        }

        if (this.ModelState.EventNumber is null)
        {
            return true;
        }

        if (versionCheckStrategy == EventVersionCheckStrategy.Monotonic)
        {
            return msg.EventNumber > this.ModelState.EventNumber.Value;
        }

        if (this.ModelState.EventNumber >= msg.EventNumber)
        {
            return false;
        }

        if (this.ModelState.EventNumber + 1 != msg.EventNumber)
        {
            var modelName = ProjectionModelName.For<TState>();
            throw new EventOutOfOrderException(
                $"{modelName} received an event out of order in stream: {msg.StreamId} ModelState state: "
                + $"{this.ModelState.EventNumber?.ToString()} eventnumber in stream: {msg.EventNumber}",
                modelName,
                this.ModelState.EventNumber.Value + 1,
                msg.EventNumber);
        }

        return true;
    }

    private void PostProcess(EventEnvelope msg)
    {
        this.ModelState.EventNumber = msg.EventNumber;
        this.ModelState.GlobalEventPosition = msg.GlobalEventPosition;
    }
}
