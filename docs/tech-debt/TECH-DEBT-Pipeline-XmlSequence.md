# Tech Debt: `<Pipeline>` Flat-Sequence Sugar for DataFlowXmlReader

## Context

The current XML structure for sequential DataFlow pipelines requires nested `<LinkTo>` elements —
each step wraps the next, growing one indent per step. With four transformations the destination
sits six levels deep. The goal is a `<Pipeline>` container that lists steps in order at a flat
level, with the reader wiring `LinkTo` calls automatically.

```xml
<!-- Before: every transformation adds one indent level -->
<CsvSource>
  <Uri>file.csv</Uri>
  <LinkTo>
    <JsonTransformation>
      <LinkTo>
        <AiTransformation>
          <LinkTo><MemoryDestination/></LinkTo>
        </AiTransformation>
      </LinkTo>
    </JsonTransformation>
  </LinkTo>
</CsvSource>

<!-- After: flat sequence inside <LinkTo> -->
<CsvSource>
  <Uri>file.csv</Uri>
  <LinkTo>
    <Pipeline>
      <JsonTransformation/>
      <AiTransformation/>
      <MemoryDestination/>
    </Pipeline>
  </LinkTo>
</CsvSource>
```

---

## Design

### Extension-point interfaces (new, in `ETLBox.Serialization`)

```
IDataFlowXmlContext        — exposes CreateStep (DI-aware), TryInvokeMethod
IExpandoXmlContext         — extends the above with RegisterDestination, RegisterErrorDestination, Destinations
IDataFlowXmlSerializable   — void ReadXml(XElement element, IDataFlowXmlContext context)
```

`DataFlowXmlReader` implements `IExpandoXmlContext`. In `CreateInstance`, one generic `if` block
checks for `IDataFlowXmlSerializable` and delegates — no hard-coded name checks, fully extensible.

The main purpose of passing `IDataFlowXmlContext` to `ReadXml` is **DI propagation**: the reader
holds an `IDataFlowActivator` (optionally backed by `IServiceProvider`), and `context.CreateStep`
routes through it. Composite components like `Pipeline` must always use `context.CreateStep` to
instantiate their children so that injected services are resolved correctly.

### Class hierarchy

```
DataFlowTransformation<TIn, TOut>         (existing base class)
  └── Pipeline<TIn, TOut>                 (new — transformation pipeline)
        └── Pipeline                      (new — ExpandoObject pipeline, also IDataFlowSource)
```

### `Pipeline<TIn, TOut>` — transformation pipeline (primary role)

- Inherits `DataFlowTransformation<TIn, TOut>` — gets all `LinkTo` overloads, `ITask` members,
  predecessor completions, and the base error handler for free
- Stores all steps in `List<object> _steps` for error forwarding
- Wires `TransformBlock = DataflowBlock.Encapsulate(head.TargetBlock, tail.SourceBlock)` —
  same pattern already used by `RowBatchTransformation`
- **Overrides `LinkErrorTo`** to forward the call to every step implementing `ILinkErrorSource`;
  a single `pipeline.LinkErrorTo(dest)` covers the entire internal chain

### `Pipeline` (non-generic) — ExpandoObject pipeline

Inherits `Pipeline<ExpandoObject, ExpandoObject>` and additionally implements
`IDataFlowSource<ExpandoObject>`, making it detectable as a root source by `DataFlowXmlReader`
when needed.

#### Special case A — source as first child

If the first child resolves to `IDataFlowSource<ExpandoObject>`, the Pipeline stores it as
`_source` and uses the **second** child as `_head`.

**Completion wiring (important):** `_source` must NOT be linked via ETLBox's `LinkTo` —
doing so would register `_source.SourceBlock.Completion` in `_head.PredecessorCompletions`,
not in `pipeline.PredecessorCompletions`. If an external upstream is also connected,
`_head.CheckCompleteAction()` would fire as soon as `_source` alone finishes, closing
`_head.TargetBlock` before the external data arrives.

Instead, wire `_source` at the raw TPL Dataflow level and register its completion in
Pipeline's own predecessor list:

```csharp
// Raw TPL link — bypasses ETLBox completion registration on _head
_source.SourceBlock.LinkTo(_head.TargetBlock);
// Register in Pipeline's predecessor list so CheckCompleteAction waits for both
AddPredecessorCompletion(_source.SourceBlock.Completion);
```

`pipeline.CheckCompleteAction()` then calls `Task.WhenAll(PredecessorCompletions)`, which
includes both the external upstream completion and `_source.SourceBlock.Completion`.
`_head.TargetBlock.Complete()` is called only after **all** sources are done.

```xml
<!-- Pipeline at root level with internal source -->
<Pipeline>
  <CsvSource><Uri>file.csv</Uri></CsvSource>
  <JsonTransformation/>
  <MemoryDestination/>
</Pipeline>

<!-- Pipeline inside <LinkTo> with external source driving it -->
<ExternalSource>
  <LinkTo>
    <Pipeline>
      <JsonTransformation/>
      <MemoryDestination/>
    </Pipeline>
  </LinkTo>
</ExternalSource>
```

`Execute()` / `ExecuteAsync()` delegate to `_source` when present; throw
`InvalidOperationException` if called with no internal source and no external driver.

#### Special case B — auto `VoidDestination` when last step has unclaimed output

If the last step implements `IDataFlowLinkSource<ExpandoObject>` and no external `LinkTo` has
been bound, `Execute()` automatically links a `VoidDestination<ExpandoObject>` and registers it
in `_dataFlow.Destinations` so the execution loop can wait for completion.

Tracking mechanism: shadow all six `LinkTo` overloads with `new` to set `_outputBound = true`
before delegating to base. `LinkTo` in `DataFlowTransformation<TIn, TOut>` is not `virtual`, so
`override` is not available. Shadowing is safe here because `Pipeline` is `sealed` and all
callers hold a `Pipeline` reference — static dispatch always resolves to the shadowing method.
At `Execute()` time, call `EnsureOutputBound()`:

```csharp
private bool _outputBound;

public new IDataFlowLinkSource<ExpandoObject> LinkTo(
    IDataFlowLinkTarget<ExpandoObject> target)
{
    _outputBound = true;
    return base.LinkTo(target);
}
// ... same for the other 5 overloads

private void EnsureOutputBound()
{
    if (_outputBound) return;
    if (_tail is IDataFlowDestination<ExpandoObject>) return;

    var sink = new VoidDestination<ExpandoObject>();
    _tail.LinkTo(sink);
    _destinationsList.Add(sink); // stored reference from ReadXml
}
```

`_destinationsList` is `IDataFlow.Destinations` — a reference stored during `ReadXml` via
`IDataFlowXmlContext`. It is a list reference, not the full context, so holding it at runtime
is appropriate.

#### `<LinkTo>` and `<LinkErrorTo>` as direct children of `<Pipeline>`

Both are method invocations, not step types. `Pipeline.ReadXml` delegates to
`context.TryInvokeMethod(this, child)` before attempting `context.CreateStep`. This mirrors
the approach already used by `DataFlowXmlReader` for all method-like XML elements — no
element names are hardcoded in `Pipeline`.

- **`<LinkTo>`** — calls `this.LinkTo(target)`, connecting Pipeline's output to the next
  component. Works identically to `<LinkTo>` on any other ETLBox component.
- **`<LinkErrorTo>`** — calls `this.LinkErrorTo(target)`, which forwards to **all** internal
  steps via the `LinkErrorTo` override in `Pipeline<TIn, TOut>`. Placement has no effect.

Elements inside the internal steps (e.g. `<JsonTransformation><LinkTo>…`) are **forbidden** —
`context.CreateStep` creates each step from its own `XElement` without following its link
children. If a step's XML contains `<LinkTo>` or `<LinkErrorTo>`, `ReadXml` throws
`InvalidDataException`.

In XML mode, `_linkAllErrorsTo` on `DataFlowXmlReader` already auto-wires each step during
`CreateInstance`, so an explicit `<LinkErrorTo>` inside `<Pipeline>` is usually not needed.

---

## Files to create / modify

| File | Change |
|------|--------|
| `ETLBox.Serialization/DataFlow/IDataFlowXmlContext.cs` | New (base interface + `IExpandoXmlContext`) |
| `ETLBox.Serialization/DataFlow/IDataFlowXmlSerializable.cs` | New |
| `ETLBox.Serialization/DataFlow/Pipeline.cs` | New (both classes) |
| `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs` | Modify — implement context + delegate |

---

## Step 1 — `IDataFlowXmlContext`

`IDataFlowXmlContext` contains only members that are not coupled to any specific row type.
ExpandoObject-specific members live in the derived `IExpandoXmlContext` so that
`Pipeline<TIn, TOut>` can use the base interface without encountering `ExpandoObject` hardcoding.

**DI wiring:** `DataFlowXmlReader` holds an `IDataFlowActivator` (backed by an optional
`IServiceProvider`). `CreateStep` delegates to `DataFlowXmlReader.CreateObject`, which calls
`_activator.CreateInstance(type)`. This means all sub-steps created via `context.CreateStep`
inside `ReadXml` are resolved through the same DI container as every other component in the
flow — **this is the primary reason `IDataFlowXmlContext` is passed to `ReadXml` at all**.
Any `ReadXml` implementation **must** use `context.CreateStep` for all sub-step instantiation;
using `new` or `Activator.CreateInstance` directly bypasses DI.

```csharp
public interface IDataFlowXmlContext
{
    /// <summary>
    /// Creates a step using the reader's activator (DI-aware). Always use this instead of
    /// new/Activator.CreateInstance to preserve dependency injection for sub-steps.
    /// </summary>
    object? CreateStep(string typeName, XElement element);
    bool TryInvokeMethod(object instance, XElement element);
}

public interface IExpandoXmlContext : IDataFlowXmlContext
{
    void RegisterDestination(IDataFlowDestination<ExpandoObject> destination);
    void RegisterErrorDestination(IDataFlowDestination<ETLBoxError> destination);
    IList<IDataFlowDestination<ExpandoObject>> Destinations { get; }
}
```

`DataFlowXmlReader` implements `IExpandoXmlContext`. `Pipeline<TIn, TOut>.ReadXml` accepts
`IDataFlowXmlContext`; `Pipeline.ReadXml` casts to `IExpandoXmlContext` for the ExpandoObject
registration calls.

## Step 2 — `IDataFlowXmlSerializable`

```csharp
public interface IDataFlowXmlSerializable
{
    void ReadXml(XElement element, IDataFlowXmlContext context);
}
```

## Step 3 — `Pipeline<TIn, TOut>`

```csharp
[PublicAPI]
public class Pipeline<TIn, TOut> : DataFlowTransformation<TIn, TOut>, IDataFlowXmlSerializable
{
    protected readonly List<object> Steps = new();
    protected IDataFlowLinkTarget<TIn>? Head;
    protected IDataFlowLinkSource<TOut>? Tail;

    protected void SetHeadAndTail(IDataFlowLinkTarget<TIn> head, IDataFlowLinkSource<TOut> tail)
    {
        Head = head;
        Tail = tail;
        TransformBlock = DataflowBlock.Encapsulate(head.TargetBlock, tail.SourceBlock);
    }

    public override void LinkErrorTo(IDataFlowLinkTarget<ETLBoxError> target)
    {
        foreach (var step in Steps)
            if (step is ILinkErrorSource src)
                src.LinkErrorTo(target);
    }

    public virtual void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        var children = element.Elements().ToList();
        if (children.Count == 0) return;

        object? prev = null;
        Type? prevOutputType = null;

        foreach (var child in children)
        {
            // Delegate method-like elements (LinkTo, LinkErrorTo, etc.) to context.
            if (context.TryInvokeMethod(this, child)) continue;

            var step = context.CreateStep(child.Name.LocalName, child)
                       ?? throw new InvalidOperationException($"...");
            Steps.Add(step);

            // Validate that step accepts the output type of the previous step.
            // GetLinkTargetInputType() finds IDataFlowLinkTarget<T> via reflection and returns T.
            var inputType = GetLinkTargetInputType(step);
            if (prevOutputType != null && inputType != prevOutputType)
                throw new InvalidDataException(
                    $"Type mismatch at '{child.Name.LocalName}': " +
                    $"expected IDataFlowLinkTarget<{prevOutputType.Name}>, got IDataFlowLinkTarget<{inputType?.Name}>");

            // Link previous step to this one via TPL (type-erased) and register completion.
            if (prev != null && prevOutputType != null)
                LinkSteps(prev, prevOutputType, step); // reflection helper

            // GetLinkSourceOutputType() finds IDataFlowLinkSource<T> and returns T.
            prevOutputType = GetLinkSourceOutputType(step);
            prev = step;
        }

        // Validate head and tail match TIn / TOut.
        var head = Steps[0] as IDataFlowLinkTarget<TIn>
                   ?? throw new InvalidDataException(
                       $"First step must implement IDataFlowLinkTarget<{typeof(TIn).Name}>");
        var tail = Steps[^1] as IDataFlowLinkSource<TOut>
                   ?? throw new InvalidDataException(
                       $"Last step must implement IDataFlowLinkSource<{typeof(TOut).Name}>");
        SetHeadAndTail(head, tail);
    }

    // Extracts T from IDataFlowLinkTarget<T> via reflection.
    private static Type? GetLinkTargetInputType(object step) =>
        step.GetType().GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType &&
                                 i.GetGenericTypeDefinition() == typeof(IDataFlowLinkTarget<>))
            ?.GetGenericArguments()[0];

    // Extracts T from IDataFlowLinkSource<T> via reflection.
    private static Type? GetLinkSourceOutputType(object step) =>
        step.GetType().GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType &&
                                 i.GetGenericTypeDefinition() == typeof(IDataFlowLinkSource<>))
            ?.GetGenericArguments()[0];

    // Links source.SourceBlock → target.TargetBlock via TPL using reflection (type-erased).
    // Registers target.AddPredecessorCompletion so CheckCompleteAction works correctly.
    private static void LinkSteps(object source, Type itemType, object target)
    {
        // Equivalent to (at runtime):
        //   var sourceBlock = ((IDataFlowLinkSource<T>)source).SourceBlock;
        //   var targetBlock = ((IDataFlowLinkTarget<T>)target).TargetBlock;
        //   sourceBlock.LinkTo(targetBlock);
        //   ((IDataFlowLinkTarget<T>)target).AddPredecessorCompletion(sourceBlock.Completion);
        var linkSourceType = typeof(IDataFlowLinkSource<>).MakeGenericType(itemType);
        var linkTargetType = typeof(IDataFlowLinkTarget<>).MakeGenericType(itemType);
        var sourceBlock = linkSourceType.GetProperty("SourceBlock")!.GetValue(source);
        var targetBlock = linkTargetType.GetProperty("TargetBlock")!.GetValue(target);
        // ISourceBlock<T>.LinkTo(ITargetBlock<T>) via reflection
        typeof(DataflowBlock)
            .GetMethod("LinkTo", new[] { typeof(ISourceBlock<>).MakeGenericType(itemType),
                                        typeof(ITargetBlock<>).MakeGenericType(itemType) })!
            .Invoke(null, new[] { sourceBlock, targetBlock });
        // Register completion
        var completion = sourceBlock!.GetType().GetProperty("Completion")!.GetValue(sourceBlock);
        linkTargetType.GetMethod("AddPredecessorCompletion")!.Invoke(target, new[] { completion });
    }
}
```

## Step 4 — `Pipeline` (non-generic)

```csharp
[PublicAPI]
public sealed class Pipeline : Pipeline<ExpandoObject, ExpandoObject>, IDataFlowSource<ExpandoObject>
{
    private IDataFlowSource<ExpandoObject>? _source;
    private bool _outputBound;
    private IList<IDataFlowDestination<ExpandoObject>>? _destinationsList;

    // IDataFlowSource
    public void Execute(CancellationToken cancellationToken = default)
    {
        EnsureOutputBound();
        _source?.Execute(cancellationToken);
    }

    public Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        EnsureOutputBound();
        return _source?.ExecuteAsync(cancellationToken)
            ?? throw new InvalidOperationException(
                "Pipeline has no internal source. Drive it by linking an external source.");
    }

    // Shadow all 6 LinkTo overloads with `new` to set _outputBound.
    // `LinkTo` in DataFlowTransformation<TIn,TOut> is not virtual, so override is unavailable.
    // Shadowing is safe: Pipeline is sealed, all callers hold a Pipeline reference.
    public new IDataFlowLinkSource<ExpandoObject> LinkTo(
        IDataFlowLinkTarget<ExpandoObject> target)
    {
        _outputBound = true;
        return base.LinkTo(target);
    }
    // ... remaining 5 overloads identical pattern

    private void EnsureOutputBound()
    {
        if (_outputBound || Tail is IDataFlowDestination<ExpandoObject>) return;
        if (Tail == null) return;

        var sink = new VoidDestination<ExpandoObject>();
        Tail.LinkTo(sink);
        _destinationsList?.Add(sink);
        _outputBound = true;
    }

    public override void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        var expandoCtx = context as IExpandoXmlContext
            ?? throw new InvalidOperationException(
                "Pipeline requires an IExpandoXmlContext implementation.");
        _destinationsList = expandoCtx.Destinations;
        var children = element.Elements().ToList();
        if (children.Count == 0) return;

        var firstType = GetTypeByName(children[0]);  // resolved via context
        int stepStart = 0;

        // Special case A: first child is a source
        if (IsSourceType(firstType))
        {
            _source = context.CreateStep(children[0].Name.LocalName, children[0])
                          as IDataFlowSource<ExpandoObject>
                      ?? throw new InvalidDataException("...");
            Steps.Add(_source);
            stepStart = 1;
        }

        // Wire remaining children as transformation/destination steps.
        // tail starts as null — _source is linked via raw TPL (see below), not via LinkTo.
        IDataFlowLinkSource<ExpandoObject>? tail = null;
        for (int i = stepStart; i < children.Count; i++)
        {
            var child = children[i];

            // Delegate method-like elements (LinkTo, LinkErrorTo, etc.) to context.
            if (context.TryInvokeMethod(this, child)) continue;

            var step = context.CreateStep(child.Name.LocalName, child)
                       ?? throw new InvalidOperationException($"...");
            Steps.Add(step);

            if (step is not IDataFlowLinkTarget<ExpandoObject> linkTarget)
                throw new InvalidDataException($"...");

            if (tail != null)
            {
                tail.LinkTo(linkTarget);
            }
            else if (_source != null)
            {
                // Raw TPL link: bypasses ETLBox completion registration on _head so that
                // _source.SourceBlock.Completion goes into Pipeline's PredecessorCompletions,
                // not into _head.PredecessorCompletions. This prevents the race condition where
                // _head closes before an external upstream finishes.
                _source.SourceBlock.LinkTo(linkTarget.TargetBlock);
                AddPredecessorCompletion(_source.SourceBlock.Completion);
            }

            if (step is IDataFlowDestination<ExpandoObject> dest)
                expandoCtx.RegisterDestination(dest);
            if (step is IDataFlowDestination<ETLBoxError> err)
                expandoCtx.RegisterErrorDestination(err);
            if (step is IDataFlowLinkSource<ExpandoObject> nextTail)
                tail = nextTail;
        }

        if (stepStart < children.Count)
        {
            var head = Steps[stepStart] as IDataFlowLinkTarget<ExpandoObject>
                       ?? throw new InvalidDataException("...");
            SetHeadAndTail(head, tail!);
        }
    }
}
```

## Step 5 — `DataFlowXmlReader` changes

### 5a — Implement `IDataFlowXmlContext`

```csharp
public sealed class DataFlowXmlReader : IExpandoXmlContext
{
    // IDataFlowXmlContext
    object? IDataFlowXmlContext.CreateStep(string typeName, XElement element) =>
        CreateObject(GetTypeByName(_types, typeName), element);

    bool IDataFlowXmlContext.TryInvokeMethod(object instance, XElement element)
    {
        if (GetMethod(instance, element) is null) return false;
        TryInvokeSourceMethod(instance, element);
        return true;
    }

    // IExpandoXmlContext
    IList<IDataFlowDestination<ExpandoObject>> IExpandoXmlContext.Destinations
        => _dataFlow.Destinations;

    void IExpandoXmlContext.RegisterDestination(IDataFlowDestination<ExpandoObject> dest) =>
        _dataFlow.Destinations.Add(dest);

    void IExpandoXmlContext.RegisterErrorDestination(IDataFlowDestination<ETLBoxError> err) =>
        _dataFlow.ErrorDestinations.Add(err);
}
```

### 5b — Delegate in `CreateInstance` (~line 311)

```csharp
if (instance is IDataFlowXmlSerializable xmlSerializable)
{
    xmlSerializable.ReadXml((XElement)node, this);
    ApplyLinkAllErrorsTo(instance);  // extracted helper from existing _linkAllErrorsTo block
    return instance;
}
```

---

## Tests to add

File: `ETLBox.Serialization.Tests/DataFlowPipelineTests.cs`

1. **Transformation pipeline** — source → `<Pipeline>` with 2 transforms + destination runs correctly.
2. **Internal source** — `<Pipeline>` at root level with source as first child executes and produces output.
3. **Auto-void destination** — last step is transformation with no external `LinkTo`; pipeline completes without stalling.
4. **External `LinkTo` suppresses auto-void** — output flows to external destination only.
5. **`LinkErrorTo` forwarding** — `pipeline.LinkErrorTo(dest)` routes errors from all internal steps.
6. **`<LinkErrorTo>` in XML** — `<LinkErrorTo>` child element inside `<Pipeline>` wires error routing.
7. **`_linkAllErrorsTo` in XML** — universal error destination covers all steps automatically.
8. **Non-source first step in root position** — throws informative error.
9. **Non-linkable step** — step not implementing `IDataFlowLinkTarget` throws `InvalidDataException`.
10. **Backward compat** — existing nested `<LinkTo>` XML works alongside a `<Pipeline>` flow.
11. **Generic `Pipeline<TIn, TOut>`** — typed pipeline links and propagates data; `LinkErrorTo` forwards.
12. **`IDataFlowXmlSerializable` extensibility** — custom component's `ReadXml` is delegated correctly.

---

## Verification

```bash
dotnet build ETLBox.Serialization/ETLBox.Serialization.csproj
dotnet test ETLBox.Serialization.Tests/ETLBox.Serialization.Tests.csproj --filter "Pipeline"
```
