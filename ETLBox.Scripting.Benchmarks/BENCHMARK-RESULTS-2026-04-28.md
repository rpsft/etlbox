# Expression Engine Benchmark Results — 2026-04-28

**Status:** Preliminary (smoke run). Full BenchmarkDotNet measurement pending.
**Origin:** review feedback on MR !116 — notes 84243 ("benchmark would be good") and 84246 ("Dynamic LINQ in mappings?").
**Tracking issue:** [TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md).

## TL;DR

For the predicate use case, Dynamic LINQ is the better default and Roslyn is a
specialised tool. On a single cold compile the smoke run shows Roslyn roughly
**10× slower** and **70–160× heavier on allocations** than either Dynamic LINQ
path. The gap is driven by `CSharpCompilation` + `Assembly.Load(bytes)` per
shape, which Dynamic LINQ avoids by reusing a shared persistent
`AssemblyBuilder`. On many shapes the gap is expected to widen further
(linear in N for Roslyn, near-flat for Dynamic LINQ); the `ManyShapes`
benchmark to confirm that quantitatively is in place but not yet executed.

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
| Machine | Intel Core i5-10300H @ 2.50 GHz, 1 CPU, 8 logical / 4 physical cores |
| OS | Windows 10 22H2 (10.0.19045.6456) |
| .NET SDK | 8.0.411 |
| Runtime | .NET 8.0.25 (8.0.2526.11203), X64 RyuJIT AVX2 |
| BenchmarkDotNet | 0.14.0 |
| Dynamic LINQ | System.Linq.Dynamic.Core 1.6.7 |
| Roslyn scripting | Microsoft.CodeAnalysis.CSharp.Scripting 4.8.0 |

What ran on this date:

- ColdCompileBenchmarks — **smoke (`--job Dry`, 1 iteration)**, all 6 cells (3 engines × 2 expressions) executed.
- FeatureParity tests — **full xUnit run**, 8/8 PASS.
- ManyShapesBenchmarks — **not run yet** (code in place, awaiting full BDN run).
- HeadToHeadBenchmarks — **not run yet** (code in place, awaiting full BDN run).

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

## Results — ColdCompile (smoke, `--job Dry`, 1 iteration)

| Method | Expression | Mean | Ratio | Allocated | Alloc Ratio |
|--------|-----------|----:|----:|--------:|---------:|
| Roslyn (ScriptBuilder) — fresh shape | Composite | 1,831.3 ms | **1.00** | **9,775.92 KB** | **1.000** |
| Dynamic LINQ generic typed POCO | Composite | 171.9 ms | 0.09 | 66.77 KB | 0.007 |
| Dynamic LINQ ExpandoObject — fresh shape | Composite | 183.1 ms | 0.10 | 134.62 KB | 0.014 |
| Roslyn (ScriptBuilder) — fresh shape | Simple | 1,668.6 ms | **1.00** | **9,716.41 KB** | **1.000** |
| Dynamic LINQ generic typed POCO | Simple | 159.4 ms | 0.10 | 57.27 KB | 0.006 |
| Dynamic LINQ ExpandoObject — fresh shape | Simple | 169.3 ms | 0.10 | 121.58 KB | 0.013 |

Reading the table:

- Roslyn cold compile is roughly **10× slower** than either Dynamic LINQ path on the same expression on the same machine.
- Roslyn allocates **70–160× more memory per shape** (~9.7 MB vs 60–125 KB).
- Expression complexity (Simple vs Composite) has marginal effect — the cost is dominated by compile + assembly emit, not by expression length.
- Dynamic LINQ on ExpandoObject is ~7–13% slower and allocates ~2× more than Dynamic LINQ on a typed POCO. The overhead is the `ExpandoTypeMapper` walk plus `DynamicClassFactory` type generation. Still two orders of magnitude below Roslyn.

**Caveat — single iteration.** `--job Dry` runs each cell exactly once. The numbers above are directional, not statistically robust. A full BDN run with warmup and several iterations is pending and will replace this section.

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

## Results — ManyShapes / HeadToHead

**Not yet measured.** Benchmark code is in place under `Benchmarks/ManyShapesBenchmarks.cs` and `Benchmarks/HeadToHeadBenchmarks.cs` and will run as part of the full BDN sweep.

Specifically:

- `ManyShapesBenchmarks` will produce, per `[Params] N ∈ {10, 50, 100}`, the time/allocation curve plus a `GlobalCleanup` probe that prints memory delta and loaded-assembly delta for each engine. This is the central measurement that confirms or refutes the assembly-leak claim made in our Round-1 answer to Q2.
- `HeadToHeadBenchmarks` will compare the shipped `ExpressionRowFiltration` against a `RowMultiplication`-based prototype (the variant the reviewer mentioned in Q1) on the same input set. If the runtime cost is identical, the case for `RowFiltration` is purely about call-site readability.

These sections will be filled in once the full run completes.

## Conclusions

Direction confirmed by smoke data; numbers will be re-stated with statistical
confidence after the full BDN run.

### Quick takeaways

1. **Cold compile cost gap is real and large.** Roughly 10× in time and 70–160×
   in allocation per shape, even on a single iteration. The order matches the
   Round-1 hypothesis about Roslyn's `Assembly.Load(bytes)` per shape.

2. **Capability gap is narrower than note 84243 suggested.** Dynamic LINQ
   already covers static methods and instance methods on framework types out
   of the box. The remaining gap is method calls on user types, addressable
   via `IDynamicLinqCustomTypeProvider`.

3. **The two engines are not "two languages" in the strong sense.** For the
   predicate use case the surface is mostly the same. The pragmatic split is
   "Roslyn when you need user-type method calls without registering them,
   multi-statement bodies or async; Dynamic LINQ otherwise."

The rest of this section unpacks each takeaway with the mechanism behind the
numbers. Skip to "Bottom line" if the three points above are enough.

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

## Suggested response to Q2 (English draft)

For the MR thread on Q2 (notes 84243 / 84246). Adapt to taste; numbers above
hardware-specific.

> Smoke benchmark is in place (ETLBox.Scripting.Benchmarks project,
> BenchmarkDotNet 0.14.0, --job Dry for now; full run with warmup pending).
> Headline numbers on a single fresh-shape cold compile, Intel i5-10300H,
> .NET 8.0.25:
>
> - Roslyn: ≈1,750 ms, ≈9.7 MB allocated per shape
> - Dynamic LINQ (typed POCO): ≈165 ms, ≈60 KB allocated
> - Dynamic LINQ (ExpandoObject): ≈175 ms, ≈130 KB allocated
>
> So Roslyn is roughly 10× slower and 70–160× heavier per cold compile.
> Direction matches the Round-1 hypothesis; the ManyShapes run with the
> assembly-count probe is in place and will tighten the numbers.
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
