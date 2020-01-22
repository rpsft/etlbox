# TODO

## Data Flow

### DF - Known Issues

- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes. Write some tests!
- Check if DBMerge works properly if the constructors are not used. E.g. if the Connectionmanager is set via assignment, the underlying DBSource and DBDestination needs to be  updated.
- BeforeBulkInsert / AfterBulkInsert in connection managers is executed before *every* bulk. There should be a "ExecuteOnceBeforeBulkInsert" function, where e.g. server side settings could be set once before every bulk operation

### DF - New features

- Based on the BlockTransformation (but without storing all data in memory), there could be predefined components that do a Sum / Min / Max / Avg or other
aggregation calculation. E.g. an Aggregation component which could be used to do such operations.
- Every DBSource reads all the data from the source. For development purposes it would be benefical if only the first X rows are read from the source. A property 
`public int Limit` could be introduced, so that only the first X rows are read for a DBSource/CSVSource/JsonSource/...
- Add documentation for the new ExpandoObject/dynamic approach

## Control Flow

### CF - Known Issues

- If a tabledefintion or a List<TableColumns> is given to the CreateTableTask, and the DataType is empty (null), then a NULLReferenceException is thrown - this probably is also true if the Name of the column (or the name of the definition) is empty. There should be tests, and the exception should be much better understandable

### CF - New features

- TableDefinition: Get "dynamic" class object from TableDefintion that can be used as type object for the flow - make sure this 
does comply with the new ExpandoObject approach
- CreateTableTask.CreateOrAlter(): add functionality to alter a table (with migration if there is data in the table).
- CreateTableTask: Function for adding test data into table (depending on table definition)

## New feature

- DataFlowBatchDestination and other destination have the code 

```await TargetBlock.Completion;
CleanUp()
```

add a `.ConfigureAwait(false)` and corresponding test, e.g by writing an async test for CSVDestination. 

## v2.0.0

- switch method parameters for GetTableDefinitionFromSource(string tablanme, IConnectionManager manager) - IConnectionManager is always first
- Add documentation for ETLBox Core: add a description that ETL is needed for snowflake, and snowflake is needed for reports, and reports could be created with PowerBI
- get rid of the ExpandoObject in the definitions and only use it internally - from the outside, only use "dynamic" as type
- make dynamic as default type instead string[] (update docu, perhaps some tests)
- remove Extension handling
- remove "name" parameter from SqlTask