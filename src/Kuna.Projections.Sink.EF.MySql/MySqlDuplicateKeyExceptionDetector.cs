using Microsoft.EntityFrameworkCore;
using MySqlConnector;

namespace Kuna.Projections.Sink.EF.MySql;

internal sealed class MySqlDuplicateKeyExceptionDetector : IDuplicateKeyExceptionDetector
{
    private const int DuplicateKeyViolationError = 1062;

    public bool IsDuplicateKeyViolation(Exception exception)
    {
        if (exception is DbUpdateException { InnerException: { } innerException, })
        {
            return this.IsDuplicateKeyViolation(innerException);
        }

        return exception is MySqlException { Number: DuplicateKeyViolationError, };
    }
}
