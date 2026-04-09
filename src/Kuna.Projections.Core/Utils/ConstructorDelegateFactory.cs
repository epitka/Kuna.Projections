using System.Linq.Expressions;
using System.Reflection;
using static Kuna.Projections.Core.Utils.Extensions;

namespace Kuna.Projections.Core.Utils;

public static class ConstructorDelegateFactory
{
    public static TDelegate? CreateConstructorFunc<TDelegate>()
        where TDelegate : class
    {
        var source = GetDelegateReturnType<TDelegate>();
        var ctrArgs = GetDelegateArguments<TDelegate>();
        var constructorInfo = source.GetConstructorInfo(ctrArgs);

        if (constructorInfo == null)
        {
            return null;
        }

        var parameters = ctrArgs.GetParameterExpressionsFrom();
        var ctorParams = parameters.GetNewExprParams();
        return Expression.Lambda<TDelegate>(Expression.New(constructorInfo, ctorParams), parameters)
                         .Compile();
    }

    public static Func<TSource>? DefaultConstructor<TSource>()
    {
        return CreateConstructorFunc<Func<TSource>>();
    }

    extension(Type source)
    {
        public Func<object[], object>? CreateConstructorFunc(params Type[] ctorParams)
        {
            var constructorInfo = source.GetConstructorInfo(ctorParams);

            if (constructorInfo == null)
            {
                return null;
            }

            var argsArray = Expression.Parameter(typeof(object[]), "args");
            var paramsExpression = new Expression[ctorParams.Length];

            for (var i = 0; i < ctorParams.Length; i++)
            {
                var argType = ctorParams[i];
                paramsExpression[i] =
                    Expression.Convert(Expression.ArrayIndex(argsArray, Expression.Constant(i)), argType);
            }

            Expression returnExpression = Expression.New(constructorInfo, paramsExpression);

            if (!source.GetTypeInfo().IsClass)
            {
                returnExpression = Expression.Convert(returnExpression, typeof(object));
            }

            return Expression.Lambda<Func<object[], object>>(returnExpression, argsArray).Compile();
        }

        public TDelegate? CreateConstructorFunc<TDelegate>()
            where TDelegate : class
        {
            var ctrArgs = GetDelegateArguments<TDelegate>();
            var constructorInfo = source.GetConstructorInfo(ctrArgs);

            if (constructorInfo == null)
            {
                return null;
            }

            var parameters = ctrArgs.GetParameterExpressionsFrom();
            var ctorParams = parameters.GetNewExprParams();
            Expression returnExpression = Expression.New(constructorInfo, ctorParams);

            if (!source.GetTypeInfo().IsClass)
            {
                returnExpression = Expression.Convert(returnExpression, typeof(object));
            }

            var lambdaParams = parameters;
            return Expression.Lambda<TDelegate>(returnExpression, lambdaParams).Compile();
        }

        public Func<object>? DefaultConstructor()
        {
            return source.CreateConstructorFunc<Func<object>>();
        }
    }
}
