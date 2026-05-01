using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Kuna.Projections.Sink.EF.Npgsql;

internal sealed class NpgsqlDuplicateKeyExceptionDetector : IDuplicateKeyExceptionDetector
{
    public bool IsDuplicateKeyViolation(Exception exception)
    {
        if (exception is DbUpdateException { InnerException: { } innerException, })
        {
            return this.IsDuplicateKeyViolation(innerException);
        }

        return exception is PostgresException { SqlState: PostgresErrorCodes.UniqueViolation, };
    }
}
