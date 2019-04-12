# Todo and Bugs

## Bugs

- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes.

## Todos

### General

- If no schema name is given, the default schema (dbo) should be used

### Control Flow Tasks

- New Tasks: Add Ola Hallagren script for database maintenance (backup, restore, ...)

- RowCountTask: Adding group by and having to RowCount?

- CreateTableTask: Function for adding test data into table (depending on table definition)

### DataFlow Tasks

- Dataflow: Mapping to objects has some kind of implicit data type checks - there should be a dataflow task which explicitly type check on data? This would mean that if data is typeof object, information is extracted via reflection

### Code cleanup

- Tests: Use RowCountTask instead of SqlTask where a count is involved

- all SQL statements in Uppercase / perhaps code formating

- Integrate DataFlowExamples into current test cases