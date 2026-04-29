# Tech Debt: Expression Engine Unification

**Status:** Split applied (2026-04-29). Unification question still open.
**Created:** 2026-04-28
**Priority:** Medium
**Origin:** review feedback on MR !116 (RowFiltration / ExpressionRowFiltration), notes 84243 and 84246

## Problem

Two different engines exist for evaluating user-supplied expressions on a row:

| Component | Package | Engine | What it can express |
|-----------|---------|--------|---------------------|
| `ScriptedRowTransformation` | `ETLBox.Scripting` | Roslyn | Full C# inside the body — method calls, async, statement blocks, custom helper types |
| `ExpressionRowFiltration`   | `ETLBox.DynamicLinq` | `System.Linq.Dynamic.Core` | Comparisons, arithmetic, logical operators, member access, null checks, LINQ-style collection methods. No method calls on user types, no statements |

As of 2026-04-29 the two engines live in **separate NuGet packages** so consumers
who need only predicate filtration pull `ETLBox.DynamicLinq` and avoid the transitive
Roslyn cost. The deeper question — whether one engine can subsume the other — is
still open and tracked below.

## Why both exist today

`ExpressionRowFiltration` was added in MR !116 to cover predicate-only use cases
(filter-by-condition) without the cost profile of Roslyn:

- Roslyn compiles a fresh assembly for each new row shape and loads it via
  `Assembly.Load(bytes)`. The assembly cannot be unloaded.
- On flows with many shapes (different sets of fields per source) memory grows
  monotonically.
- A predicate does not need the full C# language. Comparisons, arithmetic, member access,
  null checks and LINQ-style collection methods cover the vast majority of cases.

Reviewer counterpoint (note 84243):

> Я не видел проблемы с компиляцией Roslyn, потому что она выполняется один раз для
> каждого типа. По идее, написать бенчмарк недолго, что думаешь?

The cost characterisation needs benchmarks before it can be used as the deciding
argument. See the related task below.

## Open questions

1. **Is the full Roslyn surface actually used in real flows?**
   Audit existing `ScriptedRowTransformation` usages (in this repo, in Common.Etl,
   in customer packages). What fraction needs method calls or statement blocks vs.
   pure expressions?

2. **Can Dynamic LINQ be extended to cover the gap?**
   `ParsingConfig.CustomTypeProvider` allows registering additional types and methods
   visible to the parser. For specific cases — e.g. `JsonNode.ToJsonString()` — a
   targeted custom type registration would let Dynamic LINQ handle calls without
   bringing in Roslyn.

3. **Drop one engine?**
   - Drop Roslyn → migrate `ScriptedRowTransformation` to Dynamic LINQ + custom type
     providers; lose the ability to write multi-statement bodies.
   - Drop Dynamic LINQ → make `ExpressionRowFiltration` a wrapper over Roslyn; revisit
     the per-shape assembly cost.
   - Keep both → name and document the boundary clearly so the choice is obvious.

4. **Apply Dynamic LINQ in mappings (note 84246)?**
   Reviewer suggests using Dynamic LINQ in places where mappings currently rely on
   property name strings (`DbMerge` `MatchColumn` / `RetrieveColumn`,
   `JsonTransformation.Mappings`, etc.). A small expression per mapping would replace
   the property-name convention. This is a separate feature, not a unification step,
   but lives in the same family of "expression evaluation" decisions.

## Related task

**Benchmark Roslyn vs Dynamic LINQ** — needed to inform the decision in question 3.
Dimensions to measure:

- Cold compile time per new shape
- Hot evaluation time on a compiled delegate
- Memory footprint after N distinct shapes (verify or refute the assembly accumulation
  claim)
- Memory footprint after N evaluations on the same shape

Filed as a follow-up to MR !116. Without numbers the discussion in question 3 cannot be
closed.

## Out of scope for MLRSSL-1610

This file documents the question. The decision is intentionally deferred:

- MR !116 ships the two engines side by side with documentation that names the
  boundary (see `docs/dataflow/row-filtration.md`, "When to use this vs
  ScriptedRowTransformation").
- A separate task will cover the benchmark and the decision.
- A separate task will cover the mapping-expression idea (note 84246).

## Decision: split into a separate package (applied 2026-04-29)

Following the benchmark results below and reviewer feedback on MR !116, the
Dynamic LINQ-based filtration was extracted into a dedicated package
**`ETLBox.DynamicLinq`**.

What landed:

- `ETLBox.Scripting` keeps the Roslyn-based components (`ScriptedRowTransformation`,
  `ScriptBuilder`, `TypedScriptBuilder`, `EtlBoxScriptingServiceCollectionExtensions`)
  and its `Microsoft.CodeAnalysis.CSharp.Scripting` dependency. The
  `System.Linq.Dynamic.Core` dependency was removed.
- `ETLBox.DynamicLinq` (new) hosts `ExpressionRowFiltration<TInput>`,
  `ExpressionRowFiltration` (non-generic), `ExpandoTypeMapper` (internal), and
  `EtlBoxDynamicLinqServiceCollectionExtensions`. Depends on `ETLBox`,
  `ETLBox.Common` and `System.Linq.Dynamic.Core`. Namespace: `ALE.ETLBox.DynamicLinq`.
- `ETLBox.DynamicLinq.Tests` (new) hosts `ExpressionRowFiltrationTests` (37 tests)
  and `FeatureParity/MethodCallSupportTests` (8 tests). Both moved from
  `ETLBox.Scripting.Tests`.
- `ETLBox.Serialization.Tests/ExpressionRowFiltrationDeserializationTests.cs`
  references `ALE.ETLBox.DynamicLinq` instead of `ALE.ETLBox.Scripting`.
- `ETLBox.DynamicLinq.Benchmarks` (renamed from `ETLBox.Scripting.Benchmarks`)
  references both packages — Roslyn comparisons through `ETLBox.Scripting` and
  Dynamic LINQ paths through `ETLBox.DynamicLinq`.

Test counts after split:

| Project | Pass | Notes |
|---------|------|-------|
| `TestTransformations` | 11/11 | Core `RowFiltration` (no Dynamic LINQ) |
| `ETLBox.DynamicLinq.Tests` | 45/45 | 37 expression filtration + 8 feature-parity |
| `ETLBox.Scripting.Tests` | 13/13 | Roslyn (`ScriptBuilder`, `ScriptedRowTransformation`) |
| `ETLBox.Serialization.Tests` (filter) | 4/4 | XML deserialization + end-to-end pipeline |
| **Total** | **73/73** | |

The unification question — whether one engine can replace the other — remains
open (questions 1-4 above). Until that lands, the two packages coexist with the
boundary documented in `docs/dataflow/row-filtration.md` ("When to use this vs
ScriptedRowTransformation").

## Suggested next steps

1. Run the benchmark, attach numbers to this document.
2. Audit existing `ScriptedRowTransformation` usages.
3. Based on (1) and (2), pick a direction: keep both, drop one, or extend Dynamic LINQ.
4. If keeping both — reinforce the documentation boundary and possibly rename one
   component to make the boundary explicit (e.g. `ScriptedRowFiltration` for the
   Roslyn-based filter to mirror `ScriptedRowTransformation`).
5. Mapping-expression idea (note 84246) is a separate design discussion; track it as
   its own task once the engine question is settled.

## Benchmark Results (2026-04-28)

Full report:
[`ETLBox.DynamicLinq.Benchmarks/BENCHMARK-RESULTS-2026-04-28.md`](../../ETLBox.DynamicLinq.Benchmarks/BENCHMARK-RESULTS-2026-04-28.md).
Status: final. All three benchmarks completed with full BenchmarkDotNet
warmup + iterations.

### ColdCompile

Full BDN with warmup (x64, .NET 8.0, BenchmarkDotNet 0.14.0). Absolute timings
vary across machines; the ratio between engines is what reproduces:

| Engine | Mean (Composite) | Allocated per shape | Ratio to Roslyn |
|--------|---------------:|---------------:|----:|
| Roslyn (ScriptBuilder) | 121 ms | 9,758 KB | 1.00 |
| Dynamic LINQ (typed POCO) | 1.04 ms | 67 KB | 0.009 |
| Dynamic LINQ (ExpandoObject) | 2.82 ms | 134 KB | 0.025 |

Cold compile cost is roughly **40-120× smaller** and per-shape allocation
roughly **73-146× smaller** for Dynamic LINQ. The Q2 hypothesis about
per-shape `Assembly.Load(bytes)` is supported quantitatively at the
single-shape level. ManyShapes will quantify the linear-in-N curve directly.

### HeadToHead (answers Q1)

Shipped `ExpressionRowFiltration` vs `ExpressionRowMultiplicationPrototype`
(variant inheriting from `RowMultiplication`, same Dynamic LINQ logic):

| Variant | RowCount=10,000 Mean | Allocated |
|---------|---------------------:|----------:|
| ExpressionRowFiltration | 5,689 ms | 330.72 MB |
| ExpressionRowMultiplication prototype | 5,718 ms | 330.68 MB |
| Ratio | 1.01× | 1.00× |

Statistically indistinguishable — the dedicated component carries no runtime
cost over the `RowMultiplication` form. The case for it is call-site
readability only.

### ManyShapes (the central measurement for Q2)

Compile and evaluate a predicate on N distinct shapes per BenchmarkDotNet
iteration. `[GlobalCleanup]` probe captures the loaded-assembly count delta
and the managed-memory delta:

| Engine | N=10 Mean | N=100 Mean | Allocated N=100 | **Assembly delta N=10 / 50 / 100** |
|--------|----------:|-----------:|----------------:|-----------------------------------:|
| Roslyn | 660 ms | 7,522 ms | **969.81 MB** | **+768 / +11,328 / +22,628** |
| Dynamic LINQ Expando | 832 ms | 14,153 ms | 9.12 MB | +33 / +33 / +33 |

**Roslyn loaded-assembly count grows linearly with the number of distinct
shapes** and the assemblies are not unloadable. Dynamic LINQ on
ExpandoObject stays at exactly +33 regardless of N — `DynamicClassFactory`
emits new shape types into the same shared persistent `AssemblyBuilder`.
The Round-1 assembly-leak hypothesis is supported quantitatively.

Feature parity matrix (full xUnit run, 8/8 PASS):

| Scenario | Roslyn | Dynamic LINQ |
|----------|:------:|:------------:|
| Built-in instance method on string / DateTime | works | works |
| Static method on built-in type (`string.Format`) | works | **works (out of the box)** |
| Instance method on user type | works | needs `ParsingConfig.CustomTypeProvider` |

The "JsonNode → string" objection from note 84243 narrows from "Dynamic LINQ
cannot do method calls" to "Dynamic LINQ cannot do user-type method calls
without registration". The escape hatch is `IDynamicLinqCustomTypeProvider`.
