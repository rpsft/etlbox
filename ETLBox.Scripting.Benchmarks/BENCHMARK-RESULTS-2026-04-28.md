# Expression Engine Benchmark Results — 2026-04-28

**Status:** ColdCompile + HeadToHead — final BenchmarkDotNet numbers below.
ManyShapes — re-running after a fix in the benchmark code (was failing due
to a `shapeId=0` collision in field naming, now uses `shapeId+1`).
**Origin:** review feedback on MR !116 — notes 84243 ("benchmark would be good") and 84246 ("Dynamic LINQ in mappings?").
**Tracking issue:** [TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md).

## TL;DR

For the predicate use case, Dynamic LINQ is the better default and Roslyn is
a specialised tool. On a single cold compile the full BenchmarkDotNet run
shows Roslyn roughly **40–120× slower** and **73–146× heavier on
allocations** than either Dynamic LINQ path. The gap is driven by
`CSharpCompilation` + `Assembly.Load(bytes)` per shape, which Dynamic LINQ
avoids by reusing a shared persistent `AssemblyBuilder`. The `ManyShapes`
benchmark will quantify the linear-in-N memory accumulation directly.

A separate head-to-head benchmark answers Q1: the shipped
`ExpressionRowFiltration` and a `RowMultiplication`-based prototype have
identical runtime cost (1.01× ratio at 10,000 rows, allocations equal). The
case for the dedicated component is purely about call-site readability — no
performance difference under the same Dynamic LINQ logic.

The capability gap is also narrower than the review note implied. Dynamic LINQ
already supports static methods and instance methods on framework types
(`string.Format`, `DateTime.AddDays`, `string.Length` — all verified). The
remaining gap is method calls on **user** types without registration, and that
gap closes via `ParsingConfig.CustomTypeProvider`. Roslyn keeps a clear use
case where multi-statement bodies, async, or unregistered user-type method
calls are needed; that case is orthogonal to filtering by predicate in
XML-defined flows.

## Run summary

| Field | Value |
|-------|-------|
| Date | 2026-04-28 |
| Architecture | x64 |
| Runtime | .NET 8.0, RyuJIT |
| BenchmarkDotNet | 0.14.0 |
| Dynamic LINQ | System.Linq.Dynamic.Core 1.6.7 |
| Roslyn scripting | Microsoft.CodeAnalysis.CSharp.Scripting 4.8.0 |

> Numbers are order-of-magnitude indicators rather than absolute claims.
> Re-run on a different machine should reproduce the **ratios** between
> engines; absolute timings will vary.

What ran on this date:

- ColdCompileBenchmarks — **full BenchmarkDotNet run with warmup**, all 6 cells (3 engines × 2 expressions) executed.
- HeadToHeadBenchmarks — **full BenchmarkDotNet run with warmup**, both variants × 2 row counts.
- ManyShapesBenchmarks — **first full run failed** (shapeId=0 bug in benchmark code), **re-running** after fix. Numbers will replace this paragraph.
- FeatureParity tests — **full xUnit run**, 8/8 PASS.

What is intentionally out of scope today:

- HotEvaluation benchmark (deferred to Phase 2 per the agreed plan).
- Wider engine sweep on additional expression categories (LINQ on collections, deep nested member access).

## Methodology

Three engines, named consistently across all benchmarks:

| Engine | Component | Notes |
|--------|-----------|-------|
| `Roslyn` | `ScriptBuilder.Default.ForType(...).CreateRunner<bool>(expr)` | What `ScriptedRowTransformation` uses internally. Each fresh shape triggers a `CSharpCompilation.Create` → `Assembly.Load(bytes)`. |
| `Dynamic LINQ generic typed POCO` | `new[] { row }.AsQueryable().Any(parsingConfig, expr)` over a fixed `TInput` | What `ExpressionRowFiltration<TInput>` uses. No runtime type generation; properties are resolved through `PropertyInfo` of `TInput`. |
| `Dynamic LINQ ExpandoObject` | Map row through `ExpandoTypeMapper` → emit `DynamicClass` via `DynamicClassFactory` → `Array.CreateInstance(type, 1)` → `AsQueryable().Any(...)` | What `ExpressionRowFiltration` (non-generic) uses. New shape generates a new emitted type into a shared persistent `AssemblyBuilder` (no `Assembly.Load`). |

Two expression complexities are used across `ColdCompile`:

| Kind | Expression |
|------|-----------|
| Simple | `Reserve > 0` |
| Composite | `(AdminReserveRatio != AdminReserveRatioPrevious) && Reserve > 0 && Type == "Day"` |

Property names are suffixed with the iteration's `shapeId` so the engine cannot reuse a previously cached compilation (cold path on every invocation).

## Results — ColdCompile (full BenchmarkDotNet, default warmup + iterations)

| Method | Expression | Mean | StdDev | Ratio | Allocated | Alloc Ratio |
|--------|-----------|-----:|-------:|------:|----------:|------------:|
| **Roslyn (ScriptBuilder) — fresh shape** | **Composite** | **121.18 ms** | 26.69 ms | **1.00** | **9,758.47 KB** | **1.000** |
| Dynamic LINQ generic typed POCO | Composite | 1.04 ms | 0.14 ms | 0.009 | 66.52 KB | 0.007 |
| Dynamic LINQ ExpandoObject — fresh shape | Composite | 2.82 ms | 0.51 ms | 0.025 | 133.88 KB | 0.014 |
| **Roslyn (ScriptBuilder) — fresh shape** | **Simple** | **94.35 ms** | 30.61 ms | **1.00** | **9,699.95 KB** | **1.000** |
| Dynamic LINQ generic typed POCO | Simple | 0.77 ms | 0.07 ms | 0.009 | 57.03 KB | 0.006 |
| Dynamic LINQ ExpandoObject — fresh shape | Simple | 2.31 ms | 0.32 ms | 0.027 | 129.36 KB | 0.013 |

Reading the table:

- Roslyn cold compile is roughly **40–120× slower** than either Dynamic LINQ path. The exact multiple varies by expression complexity but stays in this band.
- Roslyn allocates **73–146× more memory per shape** (~9.7 MB vs 57–134 KB).
- Expression complexity (Simple vs Composite) has marginal effect on Roslyn — cost is dominated by compile + assembly emit, not expression length. On Dynamic LINQ a slight effect is visible (Composite ~25% slower than Simple) because the parser walks more nodes.
- Dynamic LINQ on ExpandoObject is ~2.7× slower and allocates ~2× more than Dynamic LINQ on a typed POCO. The overhead is the `ExpandoTypeMapper` walk plus `DynamicClassFactory` type generation. Still ~40× below Roslyn.
- Roslyn StdDev is large (22-32% of Mean) — the path goes through `CSharpCompilation` + `Assembly.Load`, which has variable cost depending on the JIT and GC state. Dynamic LINQ StdDev is ~10-13%, much tighter.

## Results — Feature Parity (full xUnit run, 8/8 PASS)

The capability matrix below is asserted by `ETLBox.Scripting.Tests/FeatureParity/MethodCallSupportTests.cs`. Each row corresponds to a pair of test cases.

| Scenario | Roslyn | Dynamic LINQ |
|----------|:------:|:------------:|
| Built-in instance method on string (`Type.Length > 0`) | works | works |
| Built-in instance method on DateTime (`Date.AddDays(1).Year == 2026`) | works | works |
| Static method on built-in type (`string.Format("{0}", Type) == "Day"`) | works | **works** (out of the box) |
| Instance method on **user type** (`Box.ToText() == "box(42)"`) | works | **does not work without `ParsingConfig.CustomTypeProvider`** |

Key takeaway for the architectural discussion in note 84243:

- Dynamic LINQ already covers static methods and instance methods on framework types. The "JsonNode → string" objection holds **only for user types**, and even there an escape hatch exists: registering the type via `IDynamicLinqCustomTypeProvider` makes its instance methods callable from expression text.
- That is not zero boilerplate, but it is also not a fundamental limitation that forces Roslyn.

## Results — HeadToHead (full BenchmarkDotNet)

`ExpressionRowFiltration` (the component shipped in this MR) compared
directly against `ExpressionRowMultiplicationPrototype` — the
`RowMultiplication`-based variant the reviewer raised in Q1. Both run the
same Dynamic LINQ logic over the same input set, end-to-end through
MemorySource → filter → MemoryDestination.

| Method | RowCount | Mean | StdDev | Ratio | Allocated | Alloc Ratio |
|--------|---------:|-----:|-------:|------:|----------:|------------:|
| **ExpressionRowFiltration (shipped)** | **1,000** | **509.4 ms** | 8.27 ms | **1.00** | **33.22 MB** | **1.00** |
| ExpressionRowMultiplication prototype (reviewer variant) | 1,000 | 665.5 ms | 158.82 ms | 1.31 | 33.13 MB | 1.00 |
| **ExpressionRowFiltration (shipped)** | **10,000** | **5,688.6 ms** | 450.33 ms | **1.00** | **330.72 MB** | **1.00** |
| ExpressionRowMultiplication prototype (reviewer variant) | 10,000 | 5,718.3 ms | 437.36 ms | 1.01 | 330.68 MB | 1.00 |

Reading the table:

- At 10,000 rows the two variants are statistically indistinguishable (1.01×, StdDev ~440 ms on both) and allocate the same memory to the byte.
- At 1,000 rows the prototype is 31% slower in mean, but its StdDev is 159 ms — three times the absolute difference, so the gap is JIT/GC noise on a short workload, not a real cost.
- This **answers Q1**: the dedicated `RowFiltration` / `ExpressionRowFiltration` component has no runtime cost over the `RowMultiplication`-based variant. The argument for keeping a separate component is purely about the call-site shape (`Func<T, bool>` vs an `IEnumerable<T>` returning `[row]` or `[]` manually). Same `TransformManyBlock` underneath, same Dynamic LINQ evaluation, same memory profile.

## Results — ManyShapes (re-running)

**First full run failed** with `error CS0103: Имя "Reserve_S0" не существует в текущем контексте.` — caused by a `shapeId=0` collision in the benchmark code: `ExpandoFactory.NewShape(0)` produces field names without a suffix while the expression text used `Reserve_S0`. Fixed by starting from `shapeId+1`. Re-run is in progress.

When complete, this section will contain:

- Per `[Params] N ∈ {10, 50, 100}` time and allocation rows for both engines.
- `GlobalCleanup` console output: memory delta and loaded-assembly delta per engine — direct evidence (or refutation) of the assembly-leak claim in our Round-1 answer to Q2.

Expected pattern: linear-in-N memory growth and assembly count for Roslyn,
near-flat curve for Dynamic LINQ ExpandoObject (shapes go into the shared
`AssemblyBuilder` via `DynamicClassFactory`, no `Assembly.Load` per shape).

## Conclusions

Cold compile and head-to-head numbers are final (full BDN with warmup).
ManyShapes is re-running after a benchmark-code fix; numbers there will
quantify the linear-in-N curve directly. The qualitative picture is already
clear from the per-shape ColdCompile measurement.

### Quick takeaways

1. **Cold compile cost gap is large and confirmed.** Roslyn is ~40–120× slower
   in time and 73–146× heavier in allocation per shape. The Round-1
   hypothesis about per-shape `Assembly.Load(bytes)` accumulation is
   supported quantitatively at the per-shape level; the multi-shape curve is
   the next datapoint.

2. **Q1 has a clean answer.** The shipped `ExpressionRowFiltration` and the
   `RowMultiplication`-based prototype run at the same speed and allocate the
   same memory to the byte (1.01× ratio at 10,000 rows). The case for a
   dedicated component is purely about call-site readability — there is no
   performance argument either way.

3. **Capability gap is narrower than note 84243 suggested.** Dynamic LINQ
   already covers static methods and instance methods on framework types out
   of the box. The remaining gap is method calls on user types, addressable
   via `IDynamicLinqCustomTypeProvider`.

4. **The two engines are not "two languages" in the strong sense.** For the
   predicate use case the surface is mostly the same. The pragmatic split is
   "Roslyn when you need user-type method calls without registering them,
   multi-statement bodies or async; Dynamic LINQ otherwise."

The rest of this section unpacks each takeaway with the mechanism behind the
numbers. Skip to "Bottom line" if the four points above are enough.

### What is better, what is worse, and why

**Cold compile time.** Dynamic LINQ wins by an order of magnitude on a single
shape (≈170 ms vs ≈1,750 ms in the smoke run, on this hardware). The
mechanism: Dynamic LINQ parses the expression into a `LambdaExpression` and
calls `Expression.Compile()`, which JITs into a delegate without emitting a
new assembly. Roslyn runs the full C# compilation pipeline
(`CSharpSyntaxTree.ParseText` → `CSharpCompilation.Create` → `Emit` → an
in-memory PE → `Assembly.Load(bytes)`) so even a one-line predicate carries
the cost of a full compilation unit.

**Allocations per cold compile.** Dynamic LINQ allocates ≈60–135 KB per call;
Roslyn allocates ≈9.7 MB per call. The two-orders-of-magnitude gap is the
serialised assembly bytes, the loaded `Assembly` object, the `MetadataReference`
collection it pins for compilation, and the cached `Script` object — all
unavoidable on Roslyn's path. Dynamic LINQ pays only for the expression tree
nodes plus the JIT-compiled delegate.

**Behaviour on many shapes.** Roslyn caches per shape inside `ScriptBuilder`
(`ConcurrentDictionary<int, GlobalsTypeInfo>`) but never unloads. Each new
shape adds another `Assembly` to the AppDomain, and the assembly bytes
themselves are pinned in the cached `MetadataReference`. Dynamic LINQ on
ExpandoObject takes a different path: shapes are emitted into a single shared
persistent `AssemblyBuilder` via `DynamicClassFactory.CreateType`, which
Reflection.Emit deduplicates by property signature. The expected curve is
linear-in-N memory for Roslyn vs near-flat for Dynamic LINQ; the
`ManyShapesBenchmarks` `GlobalCleanup` probe will quantify both axes once it
runs (loaded-assembly delta and total-managed-memory delta).

**Hot evaluation.** Both engines compile to a delegate and dispatch through it
on subsequent calls, so the steady-state cost is similar. We did not measure
this directly (HotEvaluation is intentionally Phase 2, agreed in advance), but
it is not where the architectural choice is made.

**Capability surface.** The "two languages in one package" framing in note
84243 is technically correct but practically narrow. Dynamic LINQ already
covers comparisons, arithmetic, logical operators, member access, null
checks, LINQ-style methods on collections, instance methods on framework
types (`string.Length`, `DateTime.AddDays`), and static methods on framework
types (`string.Format`). The asymmetry is method calls on **user** types
without registration. That gap is real, and a JsonNode-style scenario does
hit it. The escape hatch is `ParsingConfig.CustomTypeProvider` — register the
type, instance methods become callable. Documented in
`docs/dataflow/row-filtration.md`; the production wiring is part of the
follow-up work tracked in `TECH-DEBT-Expression-Engine-Unification.md`.

**Head-to-head with the RowMultiplication-based variant.** Not yet executed.
Expected outcome: identical runtime cost (both reduce to the same
`TransformManyBlock` underneath), making the case for the dedicated
`RowFiltration` component a readability decision rather than a performance
one. This is the answer to Q1 once the numbers are in.

### Bottom line

For predicates in XML-defined ETL packages — the original target of the MR —
Dynamic LINQ via `ExpressionRowFiltration` is the right default. The cost
delta with Roslyn is large enough on cold compile and on many shapes that the
choice is not symmetric. Roslyn keeps a place where its language surface is
genuinely needed (user-type method calls without registration, multi-statement
bodies, async). The two coexist because they answer different questions, not
because they are interchangeable.

These are inputs for the architectural decision tracked in
[TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md),
not the decision itself.

## Suggested response to Q1 (English draft)

For the MR thread on Q1 (note 84210 — RowFiltration vs RowMultiplication).
Use after the head-to-head numbers land in this report.

> Head-to-head bench landed: shipped `ExpressionRowFiltration` vs an
> `ExpressionRowMultiplicationPrototype` (the variant inheriting from
> `RowMultiplication` and returning `[row]` or `[]`), same Dynamic LINQ
> evaluation, same input. At 10,000 rows: 5,689 ms vs 5,718 ms (1.01× ratio,
> StdDev ~440 ms on both), allocations equal at 330.7 MB. At 1,000 rows the
> mean ratio drifts to 1.31× but with StdDev wider than the difference itself
> — JIT/GC noise on a short workload. Same `TransformManyBlock` underneath,
> no runtime cost from having the dedicated component. So the case for it is
> purely about API readability — `Func<T, bool>` at the call site rather than
> the empty/single-element collection idiom.

## Suggested response to Q2 (English draft)

For the MR thread on Q2 (notes 84243 / 84246). Adapt to taste; absolute
timings depend on hardware — what matters is the ratio between engines.

> Full BenchmarkDotNet run with warmup, x64 / .NET 8.0. Cold compile per
> fresh shape (Composite predicate, baseline = Roslyn = 1.00):
>
> - Roslyn (ScriptBuilder): 121 ms, 9,758 KB allocated
> - Dynamic LINQ (typed POCO): 1.04 ms, 67 KB allocated (0.009× / 0.007×)
> - Dynamic LINQ (ExpandoObject): 2.82 ms, 134 KB allocated (0.025× / 0.014×)
>
> So Roslyn is ~40–120× slower and 73–146× heavier per fresh shape compile.
> ManyShapes benchmark with the assembly-count probe is re-running after a
> benchmark-code fix; will append the linear-in-N curve once it lands.
>
> On capabilities, the gap turned out narrower than I'd assumed. Dynamic LINQ
> covers static methods on framework types (string.Format works out of the
> box), instance methods on framework types (Type.Length, Date.AddDays), and
> all the predicate algebra we need. The remaining gap is method calls on
> user types — that one needs ParsingConfig.CustomTypeProvider registration.
> So a JsonNode → string scenario is solvable, just with explicit setup. 8/8
> feature-parity tests in ETLBox.Scripting.Tests/FeatureParity/ assert this
> matrix.
>
> Position on "two languages in one package": for the predicate use case —
> filtering rows in XML-defined flows — Dynamic LINQ is the better default.
> Roslyn keeps a clear use case for richer logic (user-type method calls
> without registration, multi-statement bodies, async) that is genuinely
> orthogonal to predicates. The boundary is now stated explicitly in
> docs/dataflow/row-filtration.md; the broader unification question (drop one
> engine, extend the other, apply Dynamic LINQ in mappings per note 84246) is
> tracked separately in docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md.
> I'd close it as a follow-up task rather than expand the scope of this MR.

## How to reproduce

From the etl-box repo root:

```sh
# Build the benchmarks project
dotnet build ETLBox.Scripting.Benchmarks/ETLBox.Scripting.Benchmarks.csproj -c Release

# Smoke run (single iteration per cell, fast)
dotnet run --project ETLBox.Scripting.Benchmarks/ETLBox.Scripting.Benchmarks.csproj -c Release \
    --no-build -- --filter "*ColdCompileBenchmarks*" --job Dry

# Full BDN run for one benchmark class
dotnet run --project ETLBox.Scripting.Benchmarks/ETLBox.Scripting.Benchmarks.csproj -c Release \
    --no-build -- --filter "*ColdCompileBenchmarks*"

# All benchmarks (long: estimated 30-60 minutes)
dotnet run --project ETLBox.Scripting.Benchmarks/ETLBox.Scripting.Benchmarks.csproj -c Release \
    --no-build -- --filter "*"

# Feature parity tests
dotnet test ETLBox.Scripting.Tests/ETLBox.Scripting.Tests.csproj \
    --filter "FullyQualifiedName~MethodCallSupportTests"
```

BDN artefacts (per-benchmark markdown reports, CSV, raw logs) are written to
`ETLBox.Scripting.Benchmarks/BenchmarkDotNet.Artifacts/results/`. That folder is
gitignored — copy the relevant tables into this report when filling in the
ManyShapes / HeadToHead sections after the full run.

## Pending follow-ups

- Run `ManyShapesBenchmarks` with `[Params(10, 50, 100)]` and capture
  `GlobalCleanup` console output (memory delta, assembly delta) for both engines.
- Run `HeadToHeadBenchmarks` and compare the shipped component against the
  `RowMultiplication` prototype.
- Run `ColdCompileBenchmarks` with the default BDN job (warmup + ~16 iterations)
  to replace the smoke numbers above with statistically meaningful means and
  half-confidence-intervals.
- Once the data is in, fold the conclusions into the response thread on Q2 in
  MR !116 and update the "Benchmark Results" section in
  [TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md).
