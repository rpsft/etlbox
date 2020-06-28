# TODO

## Bugs & refactorings

### Future release
- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes. Write some tests. See Github project DataflowEx for implementation how to create a predicate that always discards records not transferred.
- Now the Connection Manager have a PrepareBulkInsert/CleanUpBulkInsert method. There are missing tests that check that make use of the Modify-Db settings and verify improved performance. DbDestination modifies these server side settings only once and at then end end of all batches.
- VoidDestination: [Use a NullBlock as Target](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.dataflowblock.nulltarget?view=netcore-3.1)
- Check if SMOConnectionManager can be reinstalled again
- ODBC connection managers for MySql etc. 
- add tests that support MariaDb (there is one test failing with CreatTableTask & ComputedColumn) 

## Enhancements

- All sources (DbSource, CsvSource, etc. )  always read all the data from the source. For development purposes it would be benefical if only the first X rows are read from the source. A property `public int Limit` could be introduced, so that only the first X rows are read for a DBSource/CSVSource/JsonSource/. This is quite easy to implement as SqlTask already has the Limit property. For Csv/Json, there should be a counter on the lines within the stream reader...
- CreateTableTask.CreateOrAlter(): add functionality to alter a table (with migration if there is data in the table).
- CreateTableTask: Function for adding test data into table (depending on table definition)
- New feature: Bounded Capacity for all Buffers (separately for every component & general static property in DataFlow), to restrict buffer size and max memory consumption

## Todo
- PrimaryKeyConstrainName now is part of TableDefinition, but not read from "GetTableDefinitionFrom"
- GCPressure was detected on CSVSource - verify if CSVSource really is the root cause. (See performance tests, improve tests that uses memory as source) 
- in order to have these tests fully working, add something like MaxBufferSize as  DataFlow parameter for all DataFlowTasks and use this when creating DF components  - also have a static DefaultMaxBufferSize as Fallback value
- Compare with sql code from dbschemareader (martinjw)
- Issue with transactions and parallel write - add a better exception handling (check if transcation is already in progress), or check if another approach is feasable (perhaps multiple transactions?)
- Add Oracle support

# Odbc support:
For better Odbc, I should look at DbSchemaReader(martinjw) in github.
Currently, if not table definition is given, the current implementation of TableDefintion.FromTable name throws an exception that the table does not exists (though it does). It would be good if the connection manager would return the code how to find if a table exists. Then the normal conneciton managers would run some sql code, and the Odbc could use ADO.NET to retrieve if the table exists and to get the table definition (independent from the database).

# Cleanup
Remove SqlTask: Add task name & Comments before sql code
Make sql task name optional

# New feature
- CopyTableDefinitionTask - uses TableDefinition to retrieve the current table definiton and the creates a new table. 
Very good for testing purposes.
- Allow user to set max buffer size for buffers. E.g. for DbDestination, max buffer size could be set to 3000 rows. If buffer is full, execution stops until destination was able to write data.  Be careful: When an exception in the destination occurs, it looks like the source and previous components still read data from the source - so it could be that the previous components are not notified of the exception, and when the max buffer size is reached the execution could deadlock. 

# Oracle
Add missing tests for specific data type conversions. E.g. number(22,2) should also create the correct .net datatype. Currently the DataTypeConverter will parse it into System.String.


