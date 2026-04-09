using System.Linq.Expressions;
using System.Reflection;

namespace Kuna.Projections.Core.Utils;

public static class ReflectionHelpers
{
    private const BindingFlags BindingFlagsInternal = BindingFlags.Public | BindingFlags.NonPublic;

    public static ConstructorInfo? GetConstructorInfo(this Type source, Type[] parameterTypes)
    {
        return (source.GetConstructor(BindingFlags.Public, null, parameterTypes, null)
                ?? source.GetConstructor(BindingFlags.NonPublic, null, parameterTypes, null))
               ?? source.GetConstructor(BindingFlagsInternal | BindingFlags.Instance, null, parameterTypes, null);
    }

    public static ParameterExpression[] GetParameterExpressionsFrom(this Type[] types)
    {
        return types
               .Select(Expression.Parameter)
               .ToArray();
    }
}
