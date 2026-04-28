# Expression Engine Benchmark Results — 2026-04-28

**Status:** Preliminary (smoke run). Full BenchmarkDotNet measurement pending.
**Origin:** review feedback on MR !116 — notes 84243 ("benchmark would be good") and 84246 ("Dynamic LINQ in mappings?").
**Tracking issue:** [TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md).

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

## Preliminary conclusions (subject to full run)

Direction confirmed by smoke data, to be re-stated with statistical confidence after full BDN run:

1. **Cold compile cost gap is real and large** — the order of magnitude separating Roslyn from Dynamic LINQ in both time and allocation matches our Round-1 hypothesis. Even one cold compile of Roslyn allocates ~9.7 MB, which on N distinct shapes will accumulate quickly.

2. **Capability gap is narrower than the review note suggested** — Dynamic LINQ supports static methods and instance methods on framework types out of the box. The remaining gap is method calls on user types, addressable through `IDynamicLinqCustomTypeProvider` registration.

3. **The two engines are not "two languages" in the strong sense** — for the predicate use case the surface is mostly the same. The pragmatic split is "Roslyn when you need user-type method calls without registering them, Dynamic LINQ otherwise". This is the wording we should reinforce in `docs/dataflow/row-filtration.md`.

These are inputs for the architectural decision tracked in [TECH-DEBT-Expression-Engine-Unification.md](../docs/tech-debt/TECH-DEBT-Expression-Engine-Unification.md), not the decision itself.

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
