using Kuna.Projections.Abstractions.Models;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using System.Collections;
using System.Reflection;

namespace Kuna.Projections.Sink.MongoDB;

internal static class ClassMapRegistry
{
    private static readonly Lock SyncRoot = new();
    private static readonly IdSerializer GuidSerializer = new();
    private static readonly NullableSerializer<Guid> NullableGuidSerializer = new(GuidSerializer);
    private static readonly GlobalEventPositionSerializer GlobalEventPositionSerializer = new();
    private static bool baseModelInitialized;

    public static void EnsureInitialized<TState>()
        where TState : class, IModel, new()
    {
        lock (SyncRoot)
        {
            EnsureBaseModelInitialized();
            EnsureClassMapsInitialized(typeof(TState), []);
        }
    }

    private static void EnsureBaseModelInitialized()
    {
        if (baseModelInitialized || BsonClassMap.IsClassMapRegistered(typeof(Model)))
        {
            baseModelInitialized = true;
            return;
        }

        BsonClassMap.RegisterClassMap<Model>(
            classMap =>
            {
                classMap.AutoMap();
                classMap.SetIsRootClass(true);

                var idMemberMap = classMap.GetMemberMap(nameof(Model.Id));
                idMemberMap.SetSerializer(GuidSerializer);
                classMap.SetIdMember(idMemberMap);

                var globalEventPositionMemberMap = classMap.GetMemberMap(nameof(Model.GlobalEventPosition));
                globalEventPositionMemberMap.SetSerializer(GlobalEventPositionSerializer);
            });

        baseModelInitialized = true;
    }

    private static void EnsureClassMapsInitialized(Type type, HashSet<Type> visitedTypes)
    {
        if (!ShouldRegisterClassMap(type)
            || !visitedTypes.Add(type))
        {
            return;
        }

        if (!BsonClassMap.IsClassMapRegistered(type))
        {
            var classMap = new BsonClassMap(type);
            classMap.AutoMap();
            ConfigureMemberSerializers(classMap);
            BsonClassMap.RegisterClassMap(classMap);
        }

        foreach (var nestedType in GetNestedTypes(type))
        {
            EnsureClassMapsInitialized(nestedType, visitedTypes);
        }
    }

    private static void ConfigureMemberSerializers(BsonClassMap classMap)
    {
        foreach (var memberMap in classMap.DeclaredMemberMaps)
        {
            if (memberMap.MemberType == typeof(Guid))
            {
                memberMap.SetSerializer(GuidSerializer);
                continue;
            }

            if (memberMap.MemberType == typeof(Guid?))
            {
                memberMap.SetSerializer(NullableGuidSerializer);
                continue;
            }

            if (memberMap.MemberType == typeof(GlobalEventPosition))
            {
                memberMap.SetSerializer(GlobalEventPositionSerializer);
            }
        }
    }

    private static IEnumerable<Type> GetNestedTypes(Type declaringType)
    {
        foreach (var property in declaringType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (property.GetIndexParameters().Length != 0)
            {
                continue;
            }

            foreach (var nestedType in ExpandCandidateTypes(property.PropertyType))
            {
                yield return nestedType;
            }
        }
    }

    private static IEnumerable<Type> ExpandCandidateTypes(Type type)
    {
        var normalizedType = Nullable.GetUnderlyingType(type) ?? type;

        if (normalizedType.IsArray)
        {
            foreach (var elementType in ExpandCandidateTypes(normalizedType.GetElementType()!))
            {
                yield return elementType;
            }

            yield break;
        }

        if (normalizedType != typeof(string)
            && typeof(IEnumerable).IsAssignableFrom(normalizedType))
        {
            if (normalizedType.IsGenericType)
            {
                foreach (var genericArgument in normalizedType.GetGenericArguments())
                {
                    foreach (var nestedType in ExpandCandidateTypes(genericArgument))
                    {
                        yield return nestedType;
                    }
                }
            }

            yield break;
        }

        if (ShouldRegisterClassMap(normalizedType))
        {
            yield return normalizedType;
        }
    }

    private static bool ShouldRegisterClassMap(Type type)
    {
        return type.IsClass
               && type != typeof(string)
               && type != typeof(object)
               && !typeof(IEnumerable).IsAssignableFrom(type)
               && !type.FullName!.StartsWith("System.", StringComparison.Ordinal);
    }
}
