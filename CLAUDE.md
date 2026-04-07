# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this
repository.

## Project Overview

ETLBox.Classic (NuGet: `EtlBox.Classic`) is an open-source .NET ETL library for building data
integration pipelines. Fork of original ETLBox by Andreas Lennartz, maintained by RapidSoft. Targets
`netstandard2.0` (libraries) and `net8.0` (tests).

## Build & Test Commands

```bash
# Build entire solution
dotnet build ETLBox.sln

# Build a specific project
dotnet build ETLBox/ETLBox.csproj

# Run all tests (requires Docker databases running)
dotnet test ETLBox.sln

# Run a specific test project
dotnet test TestTransformations/TestTransformations.csproj

# Run a single test by fully qualified name
dotnet test TestTransformations/TestTransformations.csproj --filter "FullyQualifiedName~RowTransformationTests"

# Run tests excluding performance
dotnet test --filter "Category!=Performance"

# Setup test environment (Windows)
pwsh ./test/Set-Configuration.ps1 -configEnvironment localhost-win
pwsh ./test/Run-Containers.ps1
```

Test databases (Docker): SQL Server (:1433, sa/YourStrong@Passw0rd), PostgreSQL (:5432,
postgres/etlboxpassword), MySQL (:3306, root/etlboxpassword), ClickHouse (:9000).

## Architecture

Two main subsystems inside `ETLBox/src/`:

- **Data Flow** (`Toolbox/DataFlow/`) - Sources, transformations, destinations connected via
  `LinkTo()` pattern. Built on TPL Dataflow (`ActionBlock`, `BatchBlock`). Async-first with
  `Task`-based APIs.
- **Control Flow** (`Toolbox/ControlFlow/`) - DDL/DML tasks for database management (e.g.,
  `CreateTableTask.Create()`).

Key abstractions in `Definitions/`:

- `DataFlowSource<T>` / `DataFlowTransformation<T>` / `DataFlowDestination<T>` - base classes for
  pipeline components
- `IConnectionManager` - database connection abstraction (`Toolbox/ConnectionManager/`)
- `ITask` / `GenericTask` - base for all tasks

Root namespace: `ALE.ETLBox` with sub-namespaces (`ALE.ETLBox.DataFlow`, `ALE.ETLBox.ControlFlow`,
etc.).

Extension libraries follow the pattern `ETLBox.<Feature>/` (e.g., ETLBox.Kafka, ETLBox.Json,
ETLBox.Rest, ETLBox.Serialization) with corresponding `ETLBox.<Feature>.Tests/` projects.

Shared test utilities live in `TestShared/`. Tests that cannot run in parallel are in
`TestNonParallel/`.

## Commit Conventions

Conventional Commits enforced by Husky pre-commit hook. Format: `type(scope): subject`

Types: `build|feat|ci|chore|docs|fix|perf|refactor|revert|style|test`

Rules:

- 1-90 characters total
- Subject at least 4 characters after type/scope
- No trailing period or whitespace
- Hook auto-appends `Changelog:` trailer (feat->added, fix->fixed, perf->performance, other->other)

## Testing

- **Do NOT use FluentAssertions.** Use xUnit's built-in `Assert.*` methods instead.

## Code Quality

- Warnings treated as errors (except CS0618, CS1574)
- SonarAnalyzer.CSharp and WeCantSpell.Roslyn spell-checking enabled
- XML documentation required for non-test projects
- C# language version 12
- `[PublicAPI]` attribute (JetBrains.Annotations) marks public API surface
- Custom dictionary at `.directory.dic` for spell checker

## Versioning

Version comes from `.version.yml` (`PACKAGE_RELEASE` + `PACKAGE_POSTFIX`). Local builds without this
file default to `1.0.0-development`.
