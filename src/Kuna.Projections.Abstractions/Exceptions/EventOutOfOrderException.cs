namespace Kuna.Projections.Abstractions.Exceptions;

public class EventOutOfOrderException : Exception
{
    public EventOutOfOrderException(
        string message,
        string modelName,
        long expectedEventNumber,
        long receivedEventNumber)
        : base(message)
    {
        this.ModelName = modelName;
        this.ExpectedEventNumber = expectedEventNumber;
        this.ReceivedEventNumber = receivedEventNumber;
    }

    public string ModelName { get; }

    public long ExpectedEventNumber { get; }

    public long ReceivedEventNumber { get; }
}
