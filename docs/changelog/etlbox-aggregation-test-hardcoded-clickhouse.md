# Bug: GroupingUsingDynamicObject test fails on the Docker executor due to a hardcoded ClickHouse connection

> **Status: COMPLETED** (2026-06-06) — fixed in RSSL-11727, branch `bugfix/RSSL-11727-aggregation-test-hardcoded-clickhouse`

## Problem

The test `TestTransformations.AggregationTests.AggregationDynamicObjectTests.GroupingUsingDynamicObject`
opened a ClickHouse connection with a hardcoded connection string and ran a `select 1` smoke query:

```csharp
using var con = new ClickHouseConnection(
    "Host=localhost;Port=9000;Database=default;User=clickhouse;Password=Qwe123456;");
con.Open();
using var cmd = con.CreateCommand("select 1");
cmd.ExecuteScalar();
```

Two problems:

1. **The ClickHouse connection was vestigial.** The rest of the test is a pure in-memory `Aggregation`
   over `MemorySource`/`MemoryDestination<ExpandoObject>` and never used `con`. The assertion only
   reads `dest.Data`, so the `select 1` query was unrelated to what the test verifies.
2. **The host was hardcoded to `localhost:9000`.** That only works on the GitLab Kubernetes executor,
   where service containers share the pod network namespace. On the Docker executor — and when running
   the pipeline locally via glci, where ClickHouse is reachable under the `localclickhouse` service
   alias — `localhost:9000` is connection-refused, so the test failed. Other ClickHouse tests in the
   suite use the configurable connection manager (host taken from `test/config/*.json`), not a
   hardcoded `localhost`.

## Fix

Removed the unused ClickHouse connection block and the now-unused `using ClickHouse.Ado;` import. The
in-memory assertion is unchanged, and the test is now executor-agnostic (Kubernetes / Docker / local
glci).

## Verification

```bash
dotnet test TestTransformations/TestTransformations.csproj --filter "FullyQualifiedName~AggregationDynamicObjectTests"
```

Result: `Passed! - Failed: 0, Passed: 1`.

## Affected project

`TestTransformations` (test-only change; no production code or package affected).
