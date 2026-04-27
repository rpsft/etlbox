# Tech Debt: `<Pipeline>` Flat-Sequence Sugar for DataFlowXmlReader

## Context

The current XML structure for sequential DataFlow pipelines requires nested `<LinkTo>` elements —
each step wraps the next, growing one indent per step. With four transformations the destination
sits six levels deep. The goal is a `<Pipeline>` container that lists steps in order at a flat
level, with the reader wiring `LinkTo` calls automatically.

```xml
<!-- Before -->
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

<!-- After -->
<Pipeline>
  <CsvSource><Uri>file.csv</Uri></CsvSource>
  <JsonTransformation/>
  <AiTransformation/>
  <MemoryDestination/>
</Pipeline>
```

---

## Design

### Extension-point interfaces (new, in `ETLBox.Serialization`)

```
IDataFlowXmlContext        — exposes CreateStep, RegisterDestination, RegisterErrorDestination
IDataFlowXmlSerializable   — implemented by any component that owns its XML deserialization
                             void ReadXml(XElement element, IDataFlowXmlContext context)
```

`DataFlowXmlReader` implements `IDataFlowXmlContext`. The `CreateInstance` path checks for
`IDataFlowXmlSerializable` and delegates — **one generic `if` block, no hard-coded name checks**.
Any future component can self-describe its XML format.

### Class hierarchy

```
DataFlowTransformation<TIn, TOut>         (existing base class)
  └── Pipeline<TIn, TOut>                 (new — transformation pipeline)
        └── Pipeline                      (new — ExpandoObject source pipeline, also IDataFlowSource)
```

#### `Pipeline<TIn, TOut>`

- Inherits `DataFlowTransformation<TIn, TOut>` — gets all `LinkTo` overloads, error linking,
  predecessor completions, `ITask` members for free
- Implements `IDataFlowXmlSerializable`
- Uses `DataflowBlock.Encapsulate(head.TargetBlock, tail.SourceBlock)` to wire `TransformBlock`
  (same pattern as `RowBatchTransformation`)
- Steps must form a valid `TIn → ... → TOut` type chain (validated at runtime)

#### `Pipeline` (non-generic)

- Inherits `Pipeline<ExpandoObject, ExpandoObject>`
- Additionally implements `IDataFlowSource<ExpandoObject>` — adds `Execute` / `ExecuteAsync`
  delegating to the internal source
- `IsSourceType(Pipeline)` returns `true` in `DataFlowXmlReader` — no reader special-casing needed

---

## Files to create / modify

| File | Change |
|------|--------|
| `ETLBox.Serialization/DataFlow/IDataFlowXmlContext.cs` | New |
| `ETLBox.Serialization/DataFlow/IDataFlowXmlSerializable.cs` | New |
| `ETLBox.Serialization/DataFlow/Pipeline.cs` | New (both generic + non-generic) |
| `ETLBox.Serialization/DataFlow/DataFlowXmlReader.cs` | Modify — implement context + delegate |

---

## Step 1 — `IDataFlowXmlContext`

```csharp
public interface IDataFlowXmlContext
{
    object? CreateStep(string typeName, XElement element);
    void RegisterDestination(IDataFlowDestination<ExpandoObject> destination);
    void RegisterErrorDestination(IDataFlowDestination<ETLBoxError> destination);
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
    private IDataFlowLinkTarget<TIn>? _head;
    private IDataFlowLinkSource<TOut>? _tail;

    protected void SetHeadAndTail(IDataFlowLinkTarget<TIn> head, IDataFlowLinkSource<TOut> tail)
    {
        _head = head;
        _tail = tail;
        // Encapsulate wires TransformBlock used by base class TargetBlock/SourceBlock/AddPredecessorCompletion
        TransformBlock = DataflowBlock.Encapsulate(head.TargetBlock, tail.SourceBlock);
    }

    public virtual void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        // First child must implement IDataFlowLinkTarget<TIn> (head)
        // Last child must implement IDataFlowLinkSource<TOut> (tail)
        // Middle children are linked via IDataFlowLinkTarget / IDataFlowLinkSource
        // Validate and call SetHeadAndTail when chain is built
    }
}
```

## Step 4 — `Pipeline` (non-generic, ExpandoObject source)

```csharp
[PublicAPI]
public sealed class Pipeline : Pipeline<ExpandoObject, ExpandoObject>, IDataFlowSource<ExpandoObject>
{
    private IDataFlowSource<ExpandoObject>? _source;

    public void Execute(CancellationToken cancellationToken = default) =>
        _source!.Execute(cancellationToken);

    public Task ExecuteAsync(CancellationToken cancellationToken = default) =>
        _source!.ExecuteAsync(cancellationToken);

    public override void ReadXml(XElement element, IDataFlowXmlContext context)
    {
        var steps = element.Elements().ToList();
        if (steps.Count == 0) return;

        _source = context.CreateStep(steps[0].Name.LocalName, steps[0])
                      as IDataFlowSource<ExpandoObject>
                  ?? throw new InvalidDataException(
                      "First Pipeline element must be IDataFlowSource<ExpandoObject>.");

        IDataFlowLinkSource<ExpandoObject> tail = _source;

        foreach (var stepXml in steps.Skip(1))
        {
            var step = context.CreateStep(stepXml.Name.LocalName, stepXml)
                       ?? throw new InvalidOperationException(
                           $"Failed to create pipeline step '{stepXml.Name.LocalName}'.");

            if (step is not IDataFlowLinkTarget<ExpandoObject> linkTarget)
                throw new InvalidDataException(
                    $"Pipeline step '{step.GetType().Name}' must implement IDataFlowLinkTarget<ExpandoObject>.");

            tail.LinkTo(linkTarget);

            if (step is IDataFlowDestination<ExpandoObject> dest)
                context.RegisterDestination(dest);
            if (step is IDataFlowDestination<ETLBoxError> err)
                context.RegisterErrorDestination(err);
            if (step is IDataFlowLinkSource<ExpandoObject> nextTail)
                tail = nextTail;
        }

        SetHeadAndTail(_source, tail);
    }
}
```

## Step 5 — `DataFlowXmlReader` changes

### 5a — Implement `IDataFlowXmlContext`

```csharp
public sealed class DataFlowXmlReader : IDataFlowXmlContext
```

Add three explicit-interface members:
```csharp
object? IDataFlowXmlContext.CreateStep(string typeName, XElement element) =>
    CreateObject(GetTypeByName(_types, typeName), element);

void IDataFlowXmlContext.RegisterDestination(IDataFlowDestination<ExpandoObject> dest) =>
    _dataFlow.Destinations.Add(dest);

void IDataFlowXmlContext.RegisterErrorDestination(IDataFlowDestination<ETLBoxError> err) =>
    _dataFlow.ErrorDestinations.Add(err);
```

### 5b — Delegate in `CreateInstance` (~line 311)

Add before the property-iteration `foreach`:
```csharp
if (instance is IDataFlowXmlSerializable xmlSerializable)
{
    xmlSerializable.ReadXml((XElement)node, this);
    ApplyLinkAllErrorsTo(instance);  // extracted helper from existing _linkAllErrorsTo block
    return instance;
}
```

Extract the existing `_linkAllErrorsTo` tail block into `ApplyLinkAllErrorsTo(object instance)`
to avoid duplication.

---

## What is NOT supported

- `<LinkTo>` inside a `<Pipeline>` step — undefined; use nested syntax outside `<Pipeline>`.
- `<Pipeline>` nested inside another `<LinkTo>` — only recognized at the top level as a source.

---

## Tests to add

File: `ETLBox.Serialization.Tests/DataFlowPipelineTests.cs`

1. **Happy path** — source + transformation + destination runs and produces correct output.
2. **Multiple steps** — 3+ sequential steps all linked and executed correctly.
3. **`_linkAllErrorsTo`** — universal error destination is attached to each pipeline step.
4. **Non-source first step** — throws `InvalidDataException`.
5. **Non-linkable step** — step not implementing `IDataFlowLinkTarget` throws `InvalidDataException`.
6. **Backward compat** — existing nested `<LinkTo>` XML still works.
7. **Generic `Pipeline<TIn, TOut>`** — smoke-test that typed pipeline links and propagates data.
8. **`IDataFlowXmlSerializable` extensibility** — a test-only component implementing the interface
   gets its `ReadXml` delegated correctly by the reader.

---

## Verification

```bash
dotnet build ETLBox.Serialization/ETLBox.Serialization.csproj
dotnet test ETLBox.Serialization.Tests/ETLBox.Serialization.Tests.csproj --filter "Pipeline"
```
