# Row filtration

Two transformations drop rows that should not continue down the flow:

- `RowFiltration<TInput>` (in `ETLBox`) — filters by a `Func<TInput, bool>` delegate.
- `ExpressionRowFiltration<TInput>` (in `ETLBox.DynamicLinq`) — filters by a string expression evaluated through `System.Linq.Dynamic.Core`. Suited for XML-defined flows where the predicate is configured in the package, not in C# code.

Both are non-blocking. Internally they wrap a `TransformManyBlock<TInput, TInput>` that returns a single-element collection on a passing row and an empty collection otherwise.

---

## RowFiltration

Use `RowFiltration<TInput>` when the predicate is known at compile time.

```csharp
DbSource<MySimpleRow> source = new DbSource<MySimpleRow>("SourceTable");
RowFiltration<MySimpleRow> filtration = new RowFiltration<MySimpleRow>(row => row.Col1 > 0);
DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>("DestTable");

source.LinkTo(filtration);
filtration.LinkTo(dest);
```

For dynamic rows (`ExpandoObject`) use the non-generic `RowFiltration`:

```csharp
RowFiltration filtration = new RowFiltration(row => ((dynamic)row).Col1 > 0);
```

### Why a dedicated component?

`RowMultiplication` can technically filter by returning an empty array — `row => predicate ? new[] { row } : Array.Empty<T>()`. `RowFiltration` is preferred for two reasons:

- The intent reads from the name and signature. `RowMultiplication` reads as "one to N", and using it as "one to 0 or 1" is the inverse case.
- The `Func<TInput, bool>` signature avoids the boilerplate of materializing arrays at every call site, and the base class handles the try/catch around the predicate.

This is the same separation that exists between `RowDuplication` and `RowMultiplication`: the building block is the same `TransformManyBlock`, but a focused component with a focused signature makes the pipeline easier to read.

### Error handling

If the predicate throws and an error destination is linked via `LinkErrorTo`, the failing row is forwarded there with the exception attached. Without an error destination the exception propagates and stops the flow.

```csharp
ErrorMemoryDestination errorDest = new ErrorMemoryDestination();
filtration.LinkErrorTo(errorDest);
```

`null` rows are dropped silently without invoking the predicate.

---

## ExpressionRowFiltration

`ExpressionRowFiltration` lives in `ETLBox.DynamicLinq` (separate package, no Roslyn dependency). The predicate is a string parsed by `System.Linq.Dynamic.Core` into an expression tree and compiled via `Expression.Compile()`. No Roslyn, no per-shape assembly emission, no `Assembly.Load`.

Two forms:

- `ExpressionRowFiltration<TInput>` — generic, for typed POCOs.
- `ExpressionRowFiltration : ExpressionRowFiltration<ExpandoObject>` — non-generic, the form used by the XML reader.

### Typed POCO

```csharp
public class ChangeRatioRow
{
    public int AdminReserveRatio { get; set; }
    public int AdminReserveRatioPrevious { get; set; }
}

ExpressionRowFiltration<ChangeRatioRow> filtration = new ExpressionRowFiltration<ChangeRatioRow>(
    "AdminReserveRatioPrevious != AdminReserveRatio");

source.LinkTo(filtration);
filtration.LinkTo(destination);
```

Property names in the expression resolve to public properties of `TInput`.

### ExpandoObject

```csharp
ExpressionRowFiltration filtration = new ExpressionRowFiltration(
    "Reserve > 0 && Type == \"Day\"");
```

### XML

```xml
<MemorySource>
    <LinkTo>
        <ExpressionRowFiltration>
            <FilterExpression>AdminReserveRatioPrevious != AdminReserveRatio</FilterExpression>
            <LinkTo>
                <SqlCommandTransformation>
                    ...
                </SqlCommandTransformation>
            </LinkTo>
        </ExpressionRowFiltration>
    </LinkTo>
</MemorySource>
```

When the expression contains XML special characters they must be escaped: `&gt;`, `&lt;`, `&amp;&amp;`, `&quot;`. An empty `<FilterExpression></FilterExpression>` is accepted at deserialization but throws `InvalidOperationException` at runtime when the first row arrives.

### Supported in expressions

| Category | Examples |
|----------|----------|
| Comparisons | `==`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `&&`, `\|\|`, `!`, parentheses |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| String literals | `Type == "Day"` |
| Null check | `AuthLimit != null` |
| Member access — nested ExpandoObject | `Order.Total > 100`, `Owner.Address.City.Name == "Moscow"` |
| Member access — custom class | `Person.Name == "John" && Person.Age > 18` |
| Collection methods | `Items.Any(Sum > 100)`, `Lines.Count() > 5`, `Lines.Sum(Amount) > 1000`, `Tags.Contains("Premium")` |

Supported value types: `decimal`, `int`, `string`, `bool`, `DateTime`, `Guid`, `byte[]`, `null`, nested `ExpandoObject`, custom classes with public properties, homogeneous collections.

### When to use this vs ScriptedRowTransformation

`ScriptedRowTransformation` runs the body through Roslyn and gives you the full C# language: method calls, async, custom helper types, anything an inline lambda can do. The cost is that each new row shape compiles and loads a separate assembly via `Assembly.Load(bytes)`, and those assemblies are not unloadable.

`ExpressionRowFiltration` covers the common predicate cases (comparisons, arithmetic, member access, null checks, LINQ-style methods on collections) without that footprint. If you need richer behaviour — a custom method call, a string transformation, conversion of a complex type to another representation — `ScriptedRowTransformation` is still the right pick.

The two are complementary, not interchangeable. A future iteration may consolidate them; see [`docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md`](../tech-debt/TECH-DEBT-Expression-Engine-Unification.md).

### Limitations

- **Heterogeneous collections** — collections whose elements have different field sets or types throw `InvalidOperationException`. Each element in `Items` must have the same shape.
- **Empty collection with `.Any(predicate)`** — when the source `ExpandoObject` collection is empty its element type cannot be inferred, so it falls back to `List<object>`. `.Count()` and `.Any()` (no predicate) work; `.Any(predicate)` does not because there are no element properties to bind to.
- **Cyclic references** — runtime types are emitted by `DynamicClassFactory` and cannot reference themselves. ETL row data does not normally contain cycles, but if it does the mapping fails.
- **`byte[]` and `string`** — treated as scalar values, not as enumerable collections. `Tags.Contains("Premium")` works on `List<string>`; it does not work on a `string` field treated as a sequence of characters.

---

## Internals

This section describes the moving parts inside `ExpressionRowFiltration` for contributors who extend or maintain the component. Users of the public API can stop here.

### Generic path (typed `TInput`)

`new[] { row }.AsQueryable().Any(s_parsingConfig, FilterExpression)`

The compile-time element type of the array is `TInput`, so `AsQueryable()` returns `IQueryable<TInput>` and `System.Linq.Dynamic.Core` resolves field names through `PropertyInfo` of `TInput`. No runtime type generation.

### ExpandoObject path

`ExpressionRowFiltration` (non-generic) overrides `EvaluateExpression` to map the row to a runtime DynamicClass before calling `Any`:

1. `ExpandoTypeMapper.Map(row)` walks the dictionary recursively. For nested `IDictionary<string, object>` it generates a nested DynamicClass; for a homogeneous collection it generates a `List<TElement>`; for a scalar or custom class it keeps the `GetType()` of the value.
2. `DynamicClassFactory.CreateType(properties)` emits the type into a persistent `AssemblyBuilder` and caches it by property signature. Repeated rows of the same shape reuse the same type — amortised cost is low.
3. The instance is wrapped in a typed array via `Array.CreateInstance(type, 1)`. This is required: `new[] { instance }` would give `object[]` because `Activator.CreateInstance` returns `object`, and `AsQueryable()` would lose the element type.
4. `array.AsQueryable().Any(s_parsingConfig, FilterExpression)` parses the string into an expression tree, compiles to a delegate, and evaluates it.

`ParsingConfig.ConvertObjectToSupportComparison = true` lets comparison operators work on properties typed as `object` — needed for null-valued fields and for mixed numeric literals such as `Reserve > 0` when `Reserve` is `decimal`.

### Memory characteristics

Types are emitted into one shared persistent `AssemblyBuilder` (Reflection.Emit). There is no `Assembly.Load(bytes)` per shape, so the assembly is not duplicated and not pinned in memory in the way Roslyn-compiled scripts are.
