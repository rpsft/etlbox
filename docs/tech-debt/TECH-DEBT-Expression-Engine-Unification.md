# Tech Debt: Expression Engine Unification in ETLBox.Scripting

**Status:** Open
**Created:** 2026-04-28
**Priority:** Medium
**Origin:** review feedback on MR !116 (RowFiltration / ExpressionRowFiltration), notes 84243 and 84246

## Problem

`ETLBox.Scripting` currently exposes two different engines for evaluating user-supplied
expressions on a row:

| Component | Engine | What it can express |
|-----------|--------|---------------------|
| `ScriptedRowTransformation` | Roslyn | Full C# inside the body — method calls, async, statement blocks, custom helper types |
| `ExpressionRowFiltration`   | `System.Linq.Dynamic.Core` | Comparisons, arithmetic, logical operators, member access, null checks, LINQ-style collection methods. No method calls on user types, no statements |

Two languages with different capabilities living in the same package add cognitive load:
a user picking up `ETLBox.Scripting` has to learn which one to reach for in each case,
and the difference is not visible from the names.

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

## Suggested next steps

1. Run the benchmark, attach numbers to this document.
2. Audit existing `ScriptedRowTransformation` usages.
3. Based on (1) and (2), pick a direction: keep both, drop one, or extend Dynamic LINQ.
4. If keeping both — reinforce the documentation boundary and possibly rename one
   component to make the boundary explicit (e.g. `ScriptedRowFiltration` for the
   Roslyn-based filter to mirror `ScriptedRowTransformation`).
5. Mapping-expression idea (note 84246) is a separate design discussion; track it as
   its own task once the engine question is settled.

## Benchmark Results (preliminary, 2026-04-28)

Full report:
[`ETLBox.Scripting.Benchmarks/BENCHMARK-RESULTS-2026-04-28.md`](../../ETLBox.Scripting.Benchmarks/BENCHMARK-RESULTS-2026-04-28.md).
Status: smoke run (`--job Dry`, 1 iteration) plus full feature-parity matrix.
Full BDN run with warmup, ManyShapes and HeadToHead pending.

Headline numbers from the smoke run on the ColdCompile benchmark
(x64, .NET 8.0, BenchmarkDotNet 0.14.0). Absolute timings vary
across machines; the ratio between engines is what reproduces:

| Engine | Mean (Composite) | Allocated per shape | Ratio to Roslyn |
|--------|---------------:|---------------:|----:|
| Roslyn (ScriptBuilder) | 1,831 ms | 9,776 KB | 1.00 |
| Dynamic LINQ (typed POCO) | 172 ms | 67 KB | 0.09 |
| Dynamic LINQ (ExpandoObject) | 183 ms | 135 KB | 0.10 |

Direction: cold compile cost is roughly **10× smaller** and per-shape allocation
roughly **70-160× smaller** for Dynamic LINQ. The assembly-accumulation claim
will be re-stated with statistical confidence once the ManyShapes run completes.

Feature parity matrix (full xUnit run, 8/8 PASS):

| Scenario | Roslyn | Dynamic LINQ |
|----------|:------:|:------------:|
| Built-in instance method on string / DateTime | works | works |
| Static method on built-in type (`string.Format`) | works | **works (out of the box)** |
| Instance method on user type | works | needs `ParsingConfig.CustomTypeProvider` |

The "JsonNode → string" objection from note 84243 narrows from "Dynamic LINQ
cannot do method calls" to "Dynamic LINQ cannot do user-type method calls
without registration". The escape hatch is `IDynamicLinqCustomTypeProvider`.
