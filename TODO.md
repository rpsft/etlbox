# TODO

## Bugs & refactorings

### Future release
- If not everything is connected to an destination when using predicates, it can be that the dataflow never finishes. Write some tests. See Github project DataflowEx for implementation how to create a predicate that always discards records not transferred.
- BeforeBulkInsert / AfterBulkInsert in connection managers is executed before *every* bulk. This is o.k. for SqlTask.BulkInsert,
as there is not known beforehand how many batches are coming. But in DbDestination there should be an idea how much data is coming, and there should be an ExecuteOnceBeforeBulkInsert/Cleanup calls in the ConnectionManagers (including the parameter passing between the clone method). In the ExecuteOnceBeforeBulkInsert the  server side settings could be set once before every bulk operation. Also, there are missing tests that verify that DbDestination modifies these server side settings only once and at then end end of all batches.
- VoidDestination: [Use a NullBlock as Target](https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.dataflow.dataflowblock.nulltarget?view=netcore-3.1)

## Update Docu
- Aggregate
- XmlSource/XmlDestination
- DbMerge: DbMerge supports now delta loads, including a new attribute DeleteColumn to flag deletions. 
- ExpandoObject is now default
- Improving Lookup with new set of attributes to define matching and retrieving properties. Also a new Aggretion component that simplifies creating aggregates (e.g. to calculate SUM, MIN, MAX or Count or any other custom defined calculation).
- All text files source (Csv, Json, Xml) now accept either a file path OR an URL which is loaded with a HttpClient. 
- Excel source now skip blank lines

## Enhancements

- All sources (DbSource, CsvSource, etc. )  always read all the data from the source. For development purposes it would be benefical if only the first X rows are read from the source. A property `public int Limit` could be introduced, so that only the first X rows are read for a DBSource/CSVSource/JsonSource/. This is quite easy to implement as SqlTask already has the Limit property. For Csv/Json, there should be a counter on the lines within the stream reader...
- CreateTableTask.CreateOrAlter(): add functionality to alter a table (with migration if there is data in the table).
- CreateTableTask: Function for adding test data into table (depending on table definition)
- XmlDestination