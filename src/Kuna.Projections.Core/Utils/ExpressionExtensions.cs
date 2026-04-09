using System.Linq.Expressions;

namespace Kuna.Projections.Core.Utils;

public static class ExpressionExtensions
{
    public static Expression[] GetNewExprParams(this ParameterExpression[] parameters)
    {
        return parameters.Cast<Expression>()
                         .ToArray();
    }
}
