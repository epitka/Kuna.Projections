using System.Reflection;
using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Kuna.Projections.Abstractions.Models;

BenchmarkSwitcher.FromAssembly(typeof(InvocationDispatchBenchmarks).Assembly).Run(args);

[MemoryDiagnoser]
public class InvocationDispatchBenchmarks
{
    private readonly List<object> dynamicEvents = new();
    private readonly List<Event> projectionEvents = new();

    [Params(10, 100, 1_000)]
    public int EventCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        this.dynamicEvents.Clear();
        this.projectionEvents.Clear();

        for (var i = 0; i < this.EventCount; i++)
        {
            if (i % 2 == 0)
            {
                this.dynamicEvents.Add(new CounterCreated { Amount = 1, Name = "seed" });
                this.projectionEvents.Add(new CounterCreatedProjectionEvent
                {
                    Amount = 1,
                    Name = "seed",
                    CreatedOn = DateTime.UtcNow,
                    TypeName = nameof(CounterCreatedProjectionEvent),
                });
            }
            else
            {
                this.dynamicEvents.Add(new CounterIncremented { Delta = 1 });
                this.projectionEvents.Add(new CounterIncrementedProjectionEvent
                {
                    Delta = 1,
                    CreatedOn = DateTime.UtcNow,
                    TypeName = nameof(CounterIncrementedProjectionEvent),
                });
            }
        }
    }

    [Benchmark(Baseline = true)]
    public CounterState DirectCallSwitch()
    {
        var state = new CounterState();

        foreach (var ev in this.dynamicEvents)
        {
            switch (ev)
            {
                case CounterCreated created:
                    state.Apply(created);
                    break;
                case CounterIncremented incremented:
                    state.Apply(incremented);
                    break;
            }
        }

        return state;
    }

    [Benchmark]
    public CounterState StateMutatorDynamic()
    {
        var state = new CounterState();
        ulong? version = null;
        LocalStateMutator.Mutate(state, ref version, this.dynamicEvents);
        return state;
    }

    [Benchmark]
    public CounterProjectionState ProjectionMutatorReflection()
    {
        var state = new CounterProjectionState();

        foreach (var ev in this.projectionEvents)
        {
            ProjectionMutator.Mutate(state, ev);
        }

        return state;
    }

    [Benchmark]
    public CounterProjectionState ProjectionMutatorCompiledDelegate()
    {
        var state = new CounterProjectionState();

        foreach (var ev in this.projectionEvents)
        {
            ProjectionMutatorCompiled.Mutate(state, ev);
        }

        return state;
    }
}

public static class ProjectionMutator
{
    private static readonly Dictionary<Type, Dictionary<string, MethodInfo>> Cache = new();

    public static void Mutate(object state, Event @event)
    {
        var methods = GetMethods(state.GetType());
        if (!methods.TryGetValue(@event.TypeName, out var method))
        {
            throw new InvalidOperationException($"No Apply method found for {@event.TypeName} on {state.GetType().Name}.");
        }

        method.Invoke(state, new object[] { @event });
    }

    private static Dictionary<string, MethodInfo> GetMethods(Type stateType)
    {
        if (Cache.TryGetValue(stateType, out var cached))
        {
            return cached;
        }

        var methods = stateType
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(method => method.Name == "Apply")
            .Where(method => method.ReturnType == typeof(void))
            .Where(method => method.GetParameters().Length == 1)
            .Where(method => method.GetParameters()[0].ParameterType.IsAssignableTo(typeof(Event)))
            .ToDictionary(
                method => method.GetParameters()[0].ParameterType.Name,
                method => method,
                StringComparer.InvariantCulture);

        Cache[stateType] = methods;
        return methods;
    }
}

public static class ProjectionMutatorCompiled
{
    private static readonly Dictionary<Type, Dictionary<string, Action<object, Event>>> Cache = new();

    public static void Mutate(object state, Event @event)
    {
        var methods = GetMethods(state.GetType());
        if (!methods.TryGetValue(@event.TypeName, out var mutator))
        {
            throw new InvalidOperationException($"No Apply method found for {@event.TypeName} on {state.GetType().Name}.");
        }

        mutator(state, @event);
    }

    private static Dictionary<string, Action<object, Event>> GetMethods(Type stateType)
    {
        if (Cache.TryGetValue(stateType, out var cached))
        {
            return cached;
        }

        var methods = stateType
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(method => method.Name == "Apply")
            .Where(method => method.ReturnType == typeof(void))
            .Where(method => method.GetParameters().Length == 1)
            .Where(method => method.GetParameters()[0].ParameterType.IsAssignableTo(typeof(Event)))
            .ToDictionary(
                method => method.GetParameters()[0].ParameterType.Name,
                BuildDelegate,
                StringComparer.InvariantCulture);

        Cache[stateType] = methods;
        return methods;
    }

    private static Action<object, Event> BuildDelegate(MethodInfo method)
    {
        var stateParameter = Expression.Parameter(typeof(object), "state");
        var eventParameter = Expression.Parameter(typeof(Event), "event");

        var call = Expression.Call(
            Expression.Convert(stateParameter, method.DeclaringType!),
            method,
            Expression.Convert(eventParameter, method.GetParameters()[0].ParameterType));

        return Expression
            .Lambda<Action<object, Event>>(call, stateParameter, eventParameter)
            .Compile();
    }
}

public sealed class CounterCreatedProjectionEvent : Event
{
    public int Amount { get; set; }

    public string Name { get; set; } = string.Empty;
}

public sealed class CounterIncrementedProjectionEvent : Event
{
    public int Delta { get; set; }
}

public sealed class CounterProjectionState
{
    public int Value { get; set; }

    public List<string> Changes { get; } = new();

    private void Apply(CounterCreatedProjectionEvent @event)
    {
        this.Value = @event.Amount;
        this.Changes.Add(@event.Name);
    }

    private void Apply(CounterIncrementedProjectionEvent @event)
    {
        this.Value += @event.Delta;
    }
}

public sealed class CounterCreated
{
    public int Amount { get; set; }

    public string Name { get; set; } = string.Empty;
}

public sealed class CounterIncremented
{
    public int Delta { get; set; }
}

public sealed class CounterState
{
    public int Value { get; set; }

    public List<string> Changes { get; } = new();

    public void Apply(CounterCreated @event)
    {
        this.Value = @event.Amount;
        this.Changes.Add(@event.Name);
    }

    public void Apply(CounterIncremented @event)
    {
        this.Value += @event.Delta;
    }
}

// Copied from Kuna.EventSourcing.Core.Projections.StateMutator for direct comparison.
public static class LocalStateMutator
{
    public static void Mutate(object state, ref ulong? currentVersion, IEnumerable<object> events)
    {
        foreach (var @event in events)
        {
            Mutate(state, ref currentVersion, @event);
        }
    }

    public static void Mutate(dynamic state, ref ulong? currentVersion, object @event)
    {
        state.Apply((dynamic)@event);

        if (currentVersion.HasValue)
        {
            currentVersion++;
        }
        else
        {
            currentVersion = 0;
        }
    }
}
