# Change Log

All notable changes to this project will be documented in this file.

<a name="unreleased"></a>

# Unreleased

✨ Features

- New package `ETLBox.MongoDB`: `MongoChangeStreamSource<TOutput>` tails a MongoDB change stream
  and emits one record per change event. Requires a replica set deployment. Accepts a caller-provided
  `IMongoClient`, an optional aggregation `Pipeline` for server-side filtering, and (for resumable
  processing) a `CheckpointStore` + `CheckpointId`. To checkpoint, surface the change-stream resume
  token on the mapped output so the `CheckpointWriter` can commit it (see below).

- New package `ETLBox.PostgresStreaming`: `PostgresXminTailSource<TOutput>` continuously polls a
  PostgreSQL table using `xmin`-frontier polling
  (`pg_snapshot_xmin(pg_current_snapshot())`). Rows inserted by in-flight transactions are excluded
  from each batch and automatically picked up once their transaction commits. Supports cursor
  pagination via `OrderByColumns`, server-side predicate filtering via `AdditionalWhere`, and
  resumable processing via `CheckpointStore` + `CheckpointId`. To stream UPDATEs (not just INSERTs)
  the cursor column must be re-stamped on every write (e.g. a `bigint` filled by a server-side
  sequence); use a server-side value, not an app-generated one, or concurrent writers can defeat the
  frontier.

- New: at-least-once checkpointing in `ETLBox.Common.DataFlow.Streaming`.
  - `ICheckpointStore<TPosition>` (`where TPosition : IComparable<TPosition>`) persists a typed,
    monotone stream position keyed by `checkpointId` — one stream can be tailed by many independent
    consumers, each with its own checkpoint (the Kafka consumer-group model).
  - `CheckpointWriter<TInput, TPosition>` is a terminal destination placed after the real
    destination; it commits the position (extracted from the record via a `Position` selector) only
    once a record has been durably written downstream, advancing strictly forward. A crash between
    the destination write and the commit replays the record (a duplicate) rather than dropping it —
    at-least-once; consumers must be idempotent. For a co-located destination + checkpoint, call
    `CommitAsync` inside the destination's transaction for effective exactly-once.
  - `DbCheckpointStore<TPosition>` is a ready-made store over an ETLBox `IConnectionManager`
    (configurable table/key/position columns; positions stored natively).
  - The sources are load-only: they load the committed position on start and never commit it
    themselves. Implement `ICheckpointStore<TPosition>` for any backend (Redis, database, file, …).

- New: `DataFlowResources` helper class in `ETLBox.Serialization`. Provides a composable,
  thread-safe implementation of `IDataFlow` resource ownership (connection manager pool and
  disposable resource pool). Embed it as a field and delegate `GetOrAddConnectionManager` and
  `GetOrAddResource` to it to avoid re-implementing the `ConcurrentDictionary` boilerplate in every
  `IDataFlow` implementor.

- New: `IDataFlow.GetOrAddResource(string key, Func<IDisposable> factory)` and the generic
  `DataFlowExtensions.GetOrAddResource<T>` extension. `DataFlowXmlReader` now automatically
  registers any `IDisposable` component property with the owning `IDataFlow` for lifetime management.
  Components with identical XML configuration share a single instance (deduplicated by type + content
  key); all registered resources are disposed when the flow is disposed. This applies to both
  concrete class properties (e.g., `MongoClient`) and abstract/interface properties that resolve to
  an `IDisposable` implementation.

<a name="1.18.0"></a>

# 1.18.0

✨ Features

- New: flat XML sequence syntax for `ETLBox.Serialization` via `Pipeline<TIn, TOut>` and the
  non-generic `Pipeline`. A `<Pipeline>` can now list sources, transformations, and destinations in
  execution order instead of requiring deeply nested `<LinkTo>` elements. Existing nested `<LinkTo>`
  XML remains supported.

  Example:
  ```xml
  <EtlDataFlowStep>
    <MemorySource>
      <LinkTo>
        <Pipeline>
          <JsonTransformation />
          <ScriptedTransformation />
          <MemoryDestination />
        </Pipeline>
      </LinkTo>
    </MemorySource>
  </EtlDataFlowStep>
  ```

- New: `IDataFlowXmlSerializable` and `IDataFlowXmlContext` extension points in
  `ETLBox.Serialization`. Components can now take control of their XML deserialization while still
  creating child objects through the reader's DI-aware factory.

- New: `PassThrough` property on `JsonTransformation`. When `true`, all input fields are copied to
  the output before `Mappings` are applied, allowing mappings to add new fields or override copied
  ones. When `false` (default), only mapped fields are emitted.

- New: `JsonTransformation.ParseNative(string)` and native JSON object conversion. Mappings with
  `Path="$"` now return a native `ExpandoObject` instead of a JSON string, with nested objects,
  arrays, numbers, booleans, dates, and nulls converted to .NET values.

🐛 Bug Fixes

- Fixed: `JsonTransformation` now returns `null` when a JSONPath does not match any token.

- Fixed: `Pipeline` completion handling for XML flows without an external `LinkTo`. Pipeline output
  is drained automatically when needed so execution can complete without hanging.

- Fixed: root-level `<Pipeline>` execution tracking in `DataFlowXmlReader`. A pipeline used as the
  root source is registered for completion tracking even when it contains no external destination.

- Fixed: pipeline step type validation for components that implement more than one
  `IDataFlowLinkTarget<T>` interface, such as batched destinations.

- Fixed: `DataFlowXmlReader` context type resolution now catches expected lookup exceptions when a
  custom XML-serializable component probes for optional child types.

🔧 Internal

- CI package versioning now uses `GitVersion_SemVer` for NuGet packages and a separate assembly
  version with the GitLab pipeline IID.
- Updated `GitVersion.yml` branch rules for `1.18.0` prerelease and hotfix flows.
- Changed the shared C# language version setting from `12` to `latest`.

<a name="1.17.0"></a>

# 1.17.0

✨ Features

- New: `AdditionalImports` property on `ScriptedRowTransformation<TInput, TOutput>`. Accepts a list
  of namespaces to import into every `Mappings` expression — equivalent to `using` directives. For
  example, adding `"System.Text.Json"` allows writing `JsonSerializer.Serialize(…)` instead of the
  fully qualified `System.Text.Json.JsonSerializer.Serialize(…)`.

- Improvement: `AdditionalAssemblyNames` on `ScriptedRowTransformation<TInput, TOutput>` now accepts
  both file paths (e.g. `Files/MyLib.dll`) and runtime assembly names (e.g. `System.Text.Json`).
  Previously only file paths were supported, making it impossible to reference system assemblies
  already loaded in the process.

🐛 Bug Fixes

- Fixed: `AdditionalAssemblyNames` was silently ignored when using typed `TInput`/`TOutput` (i.e.
  any non-`ExpandoObject` pair). Additional assemblies were only passed to the script compiler on the
  dynamic path; the typed path omitted the `WithReferences` call, so scripts referencing types from
  external assemblies would fail to compile.

- New: `PassThrough` property on `ScriptedRowTransformation<TInput, TOutput>` (and the non-generic
  alias `ScriptedTransformation`). When `true`, all input fields are copied to the output before
  `Mappings` are applied — fields not listed in `Mappings` are preserved unchanged. `Mappings` can
  still add new fields or override copied ones. When `false` (default), only fields explicitly listed
  in `Mappings` appear in the output.

  Example XML usage (`PassThrough` mode):
  ```xml
  <ScriptedTransformation>
    <PassThrough>true</PassThrough>
    <Mappings>
      <!-- Adds new field FullName; original fields FirstName and LastName are preserved -->
      <FullName>$"{FirstName} {LastName}"</FullName>
      <!-- Overrides existing field Amount -->
      <Amount>Amount * 1.2</Amount>
    </Mappings>
    <LinkTo>
      <MemoryDestination />
    </LinkTo>
  </ScriptedTransformation>
  ```

<a name="1.16.1"></a>

# 1.16.1

🐛 Bug Fixes

- Fixed: `ArgumentOutOfRangeException` during XML deserialization of `DbMerge` when using
  `ServiceProviderActivator`. When `ILogger` was registered in DI, `ServiceProviderActivator`
  resolved `DbMerge` via the `DbMerge(ILogger)` constructor which left `BatchSize = 0`. The subsequent
  `set_TableName` immediately created an internal `DbDestination(batchSize: 0)`, which triggered
  `BatchBlock` creation with `BoundedCapacity = 0 * 3 = 0`, causing the exception.
    - `DataFlowBatchDestination.BatchSize` setter now treats `value <= 0` as "not set" (stores `null`),
      so `InitObjects` uses `DefaultBatchSize = 1000`
    - Same fix applied to `RowBatchTransformation.BatchSize` (same vulnerable pattern)
    - `DbMerge.BatchSize` changed from auto-property to backing-field property initialized to
      `DefaultBatchSize`; setting `BatchSize` after `TableName` now propagates to internal `DestinationTable`

<a name="1.16.0"></a>

# 1.16.0

✨ Features

- New: DI-based activator mode for `DataFlowXmlReader`. Introduced `IDataFlowActivator` abstraction
  with two implementations:
    - `DefaultDataFlowActivator` (wraps existing `Activator.CreateInstance()` behavior)
    - `ServiceProviderActivator` (resolves types via `IServiceProvider`, falling back to
      `ActivatorUtilities.CreateInstance` for unregistered types)
- New: `IServiceCollection` registration extensions for each ETLBox library:
    - `AddEtlBoxCore()` — registers all core sources, transformations, and destinations (open generics
      and non-generic shorthands)
    - `AddEtlBoxJson()`, `AddEtlBoxKafka()`, `AddEtlBoxRabbitMq()`, `AddEtlBoxRest()`,
      `AddEtlBoxScripting()`, `AddEtlBoxAI()`, `AddEtlBoxSerialization()`
- New: `ILogger<T>` constructor overloads added to all data flow steps (sources, transformations,
  destinations) across core and extension libraries. Base class hierarchy (`GenericTask` →
  `DataFlowTask` → intermediate bases) forwards `ILogger` via optional parameter chaining. Enables
  structured logging with proper log category resolution when components are resolved via DI.

🔧 Internal

- Removed `FluentAssertions` dependency from all test projects. All ~208 assertion calls across 9
  files migrated to xUnit `Assert`, unifying on a single assertion style across the solution.
- Refactored `DataFlowActivator` static class into `DefaultDataFlowActivator` implementing
  `IDataFlowActivator`
- `DataFlowXmlReader` now accepts an optional `IDataFlowActivator` to control how types are
  instantiated during XML deserialization
- Added `Microsoft.Extensions.DependencyInjection.Abstractions` dependency to `ETLBox.Common`
  and `ETLBox.Serialization`

<a name="1.15.5"></a>

# 1.15.5

✨ Features

- Refactoring: Moved `DataFlowBatchDestination` from EtlBox.Classic to EtlBox.Classic.Common for
  third-party developers to create batched transformations.

<a name="1.15.4"></a>

# 1.15.4

✨ Features

- Improvement: Add to `DataFlowXmlReader` in `ETLBox.Serialization` library ability to deserialize
  `IDictionary<string,object>` type from `DataFlow` XML.
- Improvement: Changed type of `PromptParameters` setting in `AIBatchTransformation`.
  `PromptParameters` now have `IDictionary<string,object>` type with custom parameters for
  liquid-based Propmpt template to use it in render directly.

<a name="1.15.3"></a>

# 1.15.3

✨ Features

- Improvement: `AIBatchTransformation` now supports `PromptParameters` string setting, that contains
  json dictionary with custom parameters for liquid-based Propmpt template.

<a name="1.15.2"></a>

# 1.15.2

✨ Features

- New library: `ETLBox.AI` to apply AI features to `DataFlow`
- New transformation: `AIBatchTransformation` to post prompt data to a [OpenAI](https://openai.com/)
  API endpoint and get results

<a name="1.13.3"></a>

# 1.13.3

✨ Features

- Improvement: Added `BoundedCapacity` to `DataFlowBatchDestination` options to restrict buffer size
  and max memory consumption

🐛 Bug Fixes

- Fixed a memory leak when connection managers were not owned and not disposed.
- Fixed a bug in `ScriptedRowTransformation` where the dependency injection was not working
  properly.

Other changes

- Moved back from [versionize](https://github.com/versionize/versionize) to scripted version bump in
  CI/CD pipeline

<a name="1.13.1"></a>

## 1.13.1 (2025-03-10)

Other changes

- Version bump and release preparation

<a name="1.13.0"></a>

## 1.13.0 (2025-03-10)

✨ Features

- Enhanced data flow process with connection manager pooling for better resource management
- Improved memory management and connection disposal

🐛 Bug Fixes

- Fixed vulnerabilities in dependencies (RSSL-10261)
- Added proper connection manager disposal to prevent memory leaks

Other changes

- Improved test debugging under .NET 8 SDK
- Updated documentation and TODO items

<a name="1.12.4"></a>

## 1.12.4 (2024-09-30)

Other changes

- Build improvements and dependency updates

<a name="1.12.3"></a>

## 1.12.3 (2024-09-28)

Other changes

- Added script to append GitLab changelog trailer to commits
- CI/CD pipeline improvements

<a name="1.12.2"></a>

## 1.12.2 (2024-09-28)

🐛 Bug Fixes

- Removed duplicating `<Version>` tags from project files

<a name="1.12.1"></a>

## 1.12.1 (2024-09-28)

Other changes

- Updated CI pipeline to handle version bump commits and renamed deploy job

<a name="1.12.0"></a>

## 1.12.0 (2024-09-28)

Other changes

- Added version bump script and updated CI pipeline configuration
- Improved CI/CD automation

<a name="1.11.11"></a>

## 1.11.11 (2024-09-26)

Other changes

- Updated CHANGELOG.md and documentation

<a name="1.11.10"></a>

## 1.11.10 (2024-09-12)

Other changes

- Minor release with internal improvements

<a name="1.11.9"></a>

## 1.11.9 (2024-08-24)

Other changes

- Minor release with internal improvements

<a name="1.11.8"></a>

## 1.11.8 (2024-08-24)

Other changes

- Minor release with internal improvements

<a name="1.11.7"></a>

## 1.11.7 (2024-08-24)

✨ Features

- New transformation: SqlRowTransformation, SqlCommandTransformation to run parametrised SQL
  queries/commands
- New transformation: KafkaTransformation producing to Kafka topics
- New transformation: RabbitMqTransformation publishing to RabbitMq queues
- Improvement: RestTransformation now returns the response body as a string and HTTP code

🐛 Bug Fixes

- DataFlowXmlReader: Fix to allow `<[CDATA[..]]>` in XML data
- DataFlowXmlReader: Added support for floating point properties
- DbRowTransformation: Fixed connection leak
- Updated dependencies with vulnerabilities

Other changes

- Migrated from manual versioning to [versionize](https://github.com/versionize/versionize)
- Update README.md

<a name="1.10.0"></a>

## 1.10.0 (2024-05-16)

✨ Features

- Added cancelation support for long running data flow processes
- New connection type: Added support for [Clickhouse](https://clickhouse.com/docs/) columnar store
- New source: Added [Kafka](https://kafka.apache.org/) topic support as a source
- New transformation: RestTransformation to post data to a REST endpoint and get results
- New transformation: JsonTransformation to evaluate Json path expressions and extract data from
  Json
- New transformation: ScriptedRowTransformation to evaluate C# expressions to transform data

Other changes

- DbTransformation renamed to DbRowTransformation (DbTransformation is kept as `Obsolete`)
- NLog replaced with Microsoft.Extensions.Logging (except when logs are written to DB table, NLog is
  kept as internal implementation)

<a name="1.9.1"></a>

## 1.9.1 (2023-06-18)

✨ Features

- Added DataFlowXmlReader, allowing saving data flow graph configuration as XML
