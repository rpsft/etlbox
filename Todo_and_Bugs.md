# Todo and Bugs

## Bugs

- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes.

## Todos

### Control Flow Tasks

- New Tasks: Add Ola Hallagren script for database maintenance (backup, restore, ...)

- RowCountTask: Adding group by and having to RowCount?

- CreateTableTask: Function for adding test data into table (depending on table definition)

### DataFlow Tasks

- Dataflow: Mapping to objects has some kind of implicit data type checks - there should be a dataflow task which explicitly type check on data? This would mean that if data is typeof object, information is extracted via reflection

### Code cleanup

- Tests: Use RowCountTask instead of SqlTask where a count is involved

- all SQL statements in Uppercase / perhaps code formating

- Refactor Dataflow classes 
  - use inheritance or compostion, but all code which conflicts with DRY needs to be moved
  - Using Inheritance: Inherit from DataFlowTask, e.g. DataFlowSource which has LinkTo method
  - Using Composition: Source-Dataflow have a ComponentLink-Instance, which LinkTo methods are used (perhaps the best approach!)

### Hot topices

- throw exception when no connection manager is passed (e.g. use the Generic Task to check if Connectionmanager or DBConnectionManager is not null)
- Witj new SqlConnectionManager(„string „) a new ConnectionString object should be created behind the scneens, to that SqlConnectionManager(string conString) is a new Constructor of SqlConnectionManager. 
- check if parameter used in SqlTask can also be used for OdbcConnection
- create test for logging on Dataflow - especially also for the `DataFlow.LoggingThresholdRows = 2;`
- DBMerge will only properly work if the constructors are used. If not, e.g. the Connectionmanager is set via assignment, the underlying DBSource and DBDestination won't get updated. 