using System.Reflection;

namespace Kuna.Projections.Core.Utils;

public static class Extensions
{
    public static Type GetDelegateReturnType<TDelegate>()
        where TDelegate : class
    {
        var delegateType = typeof(TDelegate);
        var invokeMethod = VerifyInvokeMethod(delegateType);
        return invokeMethod.ReturnType;
    }

    public static Type[] GetDelegateArguments<TDelegate>()
        where TDelegate : class
    {
        var delegateType = typeof(TDelegate);
        var invokeMethod = VerifyInvokeMethod(delegateType);
        return invokeMethod.GetParameters()
                           .Select(p => p.ParameterType)
                           .ToArray();
    }

    private static MethodInfo VerifyInvokeMethod(Type delegateType)
    {
        var invokeMethod = delegateType.GetMethod("Invoke");

        return invokeMethod == null
                   ? throw new ArgumentException($"TDelegate does not have 'Invoke' method. Check if base class is {typeof(Delegate).FullName}.")
                   : invokeMethod;
    }
}
