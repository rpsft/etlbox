# TODO

## Data Flow

### DF - Known Issues

- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes.
- Check if DBMerge works properly if the constructors are not used. E.g. if the Connectionmanager is set via assignment, the underlying DBSource and DBDestination needs to be  updated.
- BeforeBulkInsert / AfterBulkInsert in connection managers is executed before *every* bulk. There should be a "ExecuteOnceBeforeBulkInsert" function, where e.g. server side settings could be set once before every bulk operation

### DF - New features

- Based on the BlockTransformation (but without storing all data in memory), there could be predefined components that do a Sum / Min / Max / Avg or other
aggregation calculation. E.g. an Aggregation component which could be used to do such operations.

## Control Flow

### CF - Known Issues

### CF - New features

- TableDefinition: Get "dynamic" class object from TableDefintion that can be used as type object for the flow
- CreateTableTask.CreateOrAlter(): add functionality to alter a table (with migration if there is data in the table).
- CreateTableTask: Function for adding test data into table (depending on table definition)
