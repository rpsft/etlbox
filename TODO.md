# TODO

## General

### Known Issues

- throw exception when no connection manager is passed (e.g. use the Generic Task to check if Connectionmanager or DBConnectionManager is not null)

## Data Flow

### DF - Known Issues

- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes.
- DBMerge will only properly work if the constructors are used. If not, e.g. the Connectionmanager is set via assignment, the underlying DBSource and DBDestination won't get updated.
- BeforeBulkInsert / AfterBulkInsert in connection managers is executed before *every* bulk. There should be a "ExecuteOnceBeforeBulkInsert" function, where e.g. server side settings could be set once before every bulk operation

### DF - New features

- Mapping to objects has some kind of implicit data type checks - there should be a dataflow task which explicitly type check on data? This would mean that if data is typeof object, information is extracted via reflection..

### DF - Code cleanup

- create test for logging on Dataflow - especially also for the `DataFlow.LoggingThresholdRows = 2;`

## Control Flow

### CF - Known Issues

### CF - New features

- CreateTableTask: Function for adding test data into table (depending on table definition)
