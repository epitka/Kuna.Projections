using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Kuna.Projections.Worker.Kurrent_Postgres.Example;

public static class EfExtensions
{
    public static PropertyBuilder<decimal> HasMoneyPrecision(this PropertyBuilder<decimal> builder)
    {
        return builder.HasColumnType($"decimal({18},{2})");
    }

    public static PropertyBuilder<decimal?> HasMoneyPrecision(this PropertyBuilder<decimal?> builder)
    {
        return builder.HasColumnType($"decimal({18},{2})");
    }
}
