# TODO

## Enhancements
- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes. Write some tests. See Github project DataflowEx for implementation how to create a predicate that always discards records not transferred.
- Now the Connection Manager have a PrepareBulkInsert/CleanUpBulkInsert method. There are missing tests that check that make use of the Modify-Db settings and verify improved performance. DbDestination modifies these server side settings only once and at then end end of all batches.
- Check if SMOConnectionManager can be reinstalled again
- All sources (DbSource, CsvSource, etc. )  always read all the data from the source. For development purposes it would be benefical if only the first X rows are read from the source. A property `public int Limit` could be introduced, so that only the first X rows are read for a DBSource/CSVSource/JsonSource/. This is quite easy to implement as SqlTask already has the Limit property. For Csv/Json, there should be a counter on the lines within the stream reader...
- CreateTableTask.CreateOrAlter() and Migrate(): add functionality to alter a table if empty, or Migrate a table if not empty

- From PoC: Aggregation supports currently MIN/MAX/COUNT/SUM. What about strings? Something like "FirstValue" or "LastValue" or FirstNonEmpty or LastNonEmpty?

## Refactoring

- Remove SqlTask: Add task name & Comments before sql code Make sql task name optional

## Bugs

- PrimaryKeyConstrainName now is part of TableDefinition, but not read from "GetTableDefinitionFrom"
- GCPressure was detected on CSVSource - verify if CSVSource really is the root cause. (See performance tests, improve tests that uses memory as source) 

# Improved Odbc support:

This is only relevant for Unknown Odbc (or OleDb) source. For better Odbc supportl also  look at DbSchemaReader(martinjw) in github.
Currently, if not table definition is given, the current implementation of TableDefintion.FromTable name throws an exception that the table does not exists (though it does). 
For known Odbc connection (like Sql Server), the sql is known, but for the "default" odbc connection there can't be a sql to get the table definition. But this could be done using the Ado.NET schema objects. 
It would be good if the connection manager would return the code how to find if a table exists. Then the normal conneciton managers would run some sql code, and the Odbc could use ADO.NET to retrieve if the table exists and to get the table definition (independent from the database).

# New feature

- CopyTableDefinitionTask - uses TableDefinition to retrieve the current table definiton and the creates a new table. 
Very good for testing purposes.


# Oracle

Add missing tests for specific data type conversions. E.g. number(22,2) should also create the correct .net datatype. Currently the DataTypeConverter will parse it into System.String.

# Enhance Merge for UseTruncateMethod
- The current merge does suppor the "UseTruncateMethod" flag. If set to true, the table is truncated before inserted the modified data.
In theory, this will also work for the MergeModes NoDeletions && OnlyUpdates. But then the method `ReinsertTruncatedRecords` should not 
throw an exception - itstead, it should use the InputDataDict to reinsert the records that were truncated (but shouldn't be deleted.)

# Enhance Lookup Transformation
- A "partial lookup" could be implemented. In the DbMerge, this could be useful for the DbMerge (in full load with deletions enabled this probably will not,
but it should work with other Merge modes NoDeltions, Delta & OnlyUpdates )


