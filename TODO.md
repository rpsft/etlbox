# TODO

## Bugs & refactorings

### Future release
- New feature: Bounded Capacity for all Buffers (separately for every component besides `DataFlowBatchDestination` & general static property in DataFlow), to restrict buffer size and max memory consumption
- After XML deserialization most of the components need to re-initialize internal TPL structures. This is handled inconsistently in different components. There needs to be a common method (similar to existing `InitObjects`) to be called after properties are initialized, but before execution starts.
- If not everything is connected to a destination when using predicates, it can be that the dataflow never finishes. Write some tests. See [Github project DataflowEx](https://github.com/gridsum/DataflowEx) for implementation how to create a predicate that always discards records not transferred.

## Update Documentation

- Rebuild documentation with [DocFx](https://github.com/dotnet/docfx)
- Improving Lookup with new set of attributes to define matching and retrieving properties. Also a new `Aggregation` component that simplifies creating aggregates (e.g. to calculate SUM, MIN, MAX or Count or any other custom defined calculation).
- All text files source (Csv, Json, Xml) now accept either a file path OR an URL which is loaded with a HttpClient.
- Excel source now skip blank lines

## Enhancements

- CreateTableTask.CreateOrAlter(): add functionality to alter a table (with migration if there is data in the table).
- CreateTableTask: Function for adding test data into table (depending on table definition)

## Other
- PrimaryKeyConstrainName now is part of TableDefinition, but not read from `GetTableDefinitionFrom`
- in order to have these tests fully working, add something like MaxBufferSize as  DataFlow parameter for all DataFlowTasks and use this when creating DF components  - also have a static DefaultMaxBufferSize as Fallback value
