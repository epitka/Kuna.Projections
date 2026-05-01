namespace Kuna.Projections.Sink.EF;

public interface IDuplicateKeyExceptionDetector
{
    bool IsDuplicateKeyViolation(Exception exception);
}
