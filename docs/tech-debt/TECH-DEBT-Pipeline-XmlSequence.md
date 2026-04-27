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
IDataFlowXmlContext        — exposes CreateStep, RegisterDestination, RegisterErrorDestination
IDataFlowXmlSerializable   — void ReadXml(XElement element, IDataFlowXmlContext context)
```

`DataFlowXmlReader` implements `IDataFlowXmlContext`. In `CreateInstance`, one generic `if` block
checks for `IDataFlowXmlSerializable` and delegates — no hard-coded name checks, fully extensible.

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
`_source`, uses the **second** child as `_head`, and internally calls `_source.LinkTo(_head)`.
The Pipeline's external `TargetBlock` (from `_head`) still accepts external input, so data from
both the internal source and any external upstream components merges naturally into the same
transformation chain.

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

Tracking mechanism: override all six `LinkTo` overloads to set `_outputBound = true` before
delegating to base. At `Execute()` time, call `EnsureOutputBound()`:

```csharp
private bool _outputBound;

public override IDataFlowLinkSource<ExpandoObject> LinkTo(
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

#### Error linking in XML — `<LinkErrorTo>` inside `<Pipeline>`

`Pipeline.ReadXml` iterates children as steps. `<LinkErrorTo>` is not a type name — it is a
method invocation. The reader must detect it by element name and handle it via the same logic as
`TryInvokeSourceMethod`, calling `LinkErrorTo` on `_tail` (or all steps, depending on placement).

In XML mode, `_linkAllErrorsTo` already auto-wires each step during `CreateInstance`, so the
universal error destination covers all steps without any explicit XML required.

---

## Files to create / modify

| File | Change |
|------|--------|
| `ETLBox.Serialization/DataFlow/IDataFlowXmlContext.cs` | New |
| `ETLBox.Serialization/DataFlow/IDataFlowXmlSerializable.cs` | New |
| `ETLBox.Serialization/DataFlow/Pipeline.cs` | New (both classes) |
| `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs` | Modify — implement context + delegate |

---

## Step 1 — `IDataFlowXmlContext`

```csharp
public interface IDataFlowXmlContext
{
    object? CreateStep(string typeName, XElement element);
    void RegisterDestination(IDataFlowDestination<ExpandoObject> destination);
    void RegisterErrorDestination(IDataFlowDestination<ETLBoxError> destination);
    IList<IDataFlowDestination<ExpandoObject>> Destinations { get; }
}
```

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
        // First child must implement IDataFlowLinkTarget<TIn> (head)
        // Last child must implement IDataFlowLinkSource<TOut> (tail)
        // Middle children linked via IDataFlowLinkTarget / IDataFlowLinkSource
        // Validate types form a TIn → ... → TOut chain and call SetHeadAndTail
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

    // Override all 6 LinkTo overloads to set _outputBound
    public override IDataFlowLinkSource<ExpandoObject> LinkTo(
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
        _destinationsList = context.Destinations;
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

        // Wire remaining children as transformation/destination steps
        IDataFlowLinkSource<ExpandoObject>? tail = _source;
        for (int i = stepStart; i < children.Count; i++)
        {
            var child = children[i];

            // Special case: <LinkErrorTo> is a method invocation, not a step type
            if (child.Name.LocalName == "LinkErrorTo")
            {
                ProcessLinkErrorTo(child, tail, context);
                continue;
            }

            var step = context.CreateStep(child.Name.LocalName, child)
                       ?? throw new InvalidOperationException($"...");
            Steps.Add(step);

            if (step is not IDataFlowLinkTarget<ExpandoObject> linkTarget)
                throw new InvalidDataException($"...");

            tail?.LinkTo(linkTarget);

            if (step is IDataFlowDestination<ExpandoObject> dest)
                context.RegisterDestination(dest);
            if (step is IDataFlowDestination<ETLBoxError> err)
                context.RegisterErrorDestination(err);
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
public sealed class DataFlowXmlReader : IDataFlowXmlContext
{
    IList<IDataFlowDestination<ExpandoObject>> IDataFlowXmlContext.Destinations
        => _dataFlow.Destinations;

    object? IDataFlowXmlContext.CreateStep(string typeName, XElement element) =>
        CreateObject(GetTypeByName(_types, typeName), element);

    void IDataFlowXmlContext.RegisterDestination(IDataFlowDestination<ExpandoObject> dest) =>
        _dataFlow.Destinations.Add(dest);

    void IDataFlowXmlContext.RegisterErrorDestination(IDataFlowDestination<ETLBoxError> err) =>
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
