using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Kuna.Projections.Sink.EF.SqlServer;

internal sealed class SqlServerDuplicateKeyExceptionDetector : IDuplicateKeyExceptionDetector
{
    private const int DuplicatePrimaryKeyViolationError = 2627;

    public bool IsDuplicateKeyViolation(Exception exception)
    {
        if (exception is DbUpdateException { InnerException: { } innerException, })
        {
            return this.IsDuplicateKeyViolation(innerException);
        }

        return exception is SqlException { Number: DuplicatePrimaryKeyViolationError, };
    }
}
