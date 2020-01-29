# Exceution, Linking and Completion

## Linking components

Before you can execute a data flow, you need to link your source, transformation and destinations.
The linking is quite easy - every source component and every transformation offers a LinkTo() method.
This method accepts a target, which either is another transformation or a destination. 

Example of Linking a `DBSource` to a `RowTransformation` and the to a `DBDestination`.

```C#
//Create the components
DBSource source = new DBSource("SourceTable");
RowTransformation rowTrans = new RowTransformation( row => row );
DBDestination dest = new DBDestination("DestTable");

//Link the components
source.LinkTo(row);
row.LinkTo(dest);
```

This will result in a flow which looks like this:

DBSource --> RowTransformation --> DBDestination

### Fluent notation

There is also a chained notation available, which give you the same result:

```C#
//Link the components
source.LinkTo(row).LinkTo(dest);
```

This notation can be used most of the time - please note that it won't work with `Multicast` or `MergeJoin` as these
components have more than one input respective output.

If your transformation has a different output type than your input, you need to adjust the linking a little bit. The LinkTo
accepts a type that defines the output of the linking. 
E.g. if you have a `RowTransformation<InputType, OutputType> row`, the linking would look like this:

```C#
source.LinkTo<OutputType>(row).LinkTo(dest)
```

## Predicates

Whenever you link components in a dataflow, you can add a filter expression to the link -
this is called a predicate for the link.
The filter expression is evaluated for every row that goes through the link.
If the evaluated expression is true, data will pass into the linked component.
If evaluated to false, the dataflow will try the next link to send its data through.

**Note:** Data will be send only into one of the connected links. If there is more than one link,
the first link that either has no predicate or which predicate returns true is used.

If you need data send into two ore more connected components, you can use the Multicast:

```C#
source.LinkTo(multicast);
multicast.LinkTo(dest1, row => row.Value2 <= 2);
multicast.LinkTo(dest2,  row => row.Value2 > 2);
source.Execute();
dest1.Wait();
dest2.Wait();
```

### VoidDestination

Whenever you use predicates, sometime you end up with data which you don't want to write into a destination.
Unfortunately, your DataFlow won't finish until all rows where written into any destination block. That's why 
there is a `VoidDestination` in ETLBox. Use this destination for all records for that you don't wish any further processing. 

```C#
VoidDestination voidDest = new VoidDestination(); 
source.LinkTo(dest, row => row.Value > 0);
souce.Link(voidDest, row => row.Value <= 0);
```

#### Implicit use of VoidDestination

You don't have to define the `VoidDestinatin` explicitly. Assuming that we have a Database Source 
that we want to link to a database destination. But we only want to let data pass through where the 
a column is greater than 0. The rest we want to ignore. Normally, we would need to link the data twice like in 
the example above. But there is a short way to write it: 

```C#
source.LinkTo(dest, row => row.Value > 0,  row => row.Value <= 0);
```

Internally, this will create a `VoidDestination` when linking the components, but you don't have to deal with anymore.
At the end, only records where the Value column is greater 0 will be written into the destination.

## Linking errors

By default, exception won't be handled within you dataflow components. Whenever within a source, transformation or 
a destination an error occurs, this exception will be thrown in your user code. You can use the normal try/catch block to handle
these exceptions.

If you want to handle exceptions within your dataflow, some components offer the ability to redirect errors.
Beside the normal `LinkTo` method, you can use the  `LinkErrorTo` to redirect erroronous records into a separate pipeline.

Here an example for a database source, where error records are linked into a MemoryDestination:

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(connection, "SourceTable");
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(connection, "DestinationTable");
MemoryDestination<ETLBoxError> errorDest = new MemoryDestination<ETLBoxError>();
source.LinkTo(dest);
source.LinkErrorTo(errorDest);
source.Execute();
dest.Wait();
errorDest.Wait();
```

`LinkErrorTo` only accepts transformations or destinations that have the input type `ETLBoxError`. It will contain
the exception itself and an exception message, the time the error occured, and the faulted record as json (if it was
possible to connvert it).

ETLBoxError is defined like this:

```C#
public class ETLBoxError
{
    public string ErrorText { get; set; }
    public DateTime ReportTime { get; set; }
    public Exception Exception { get; set; }
    public string RecordAsJson { get; set; }
}
```

### CreateErrorTableTask

If you want to store your exception in a table in a database, ETLBox offers you a task that will automatically 
create this table for you.

```C#
CreateErrorTableTask.Create(connection, "etlbox_error");
```

The table will have three columns: ErrorText, RecordAsJson and ReportTime (with the right data type). Of course you can 
create you own table.

## Multiple inputs

There is no restriction on the amount of inputs that a destination or transformation can have. Instead of having
only single source, you can have multiple source for every component that can be linked.

E.g. this is possible graph for you dataflow:

```
DBSource1 ---> RowTransformation1 -|
DBSource2 -|-> RowTransformation2 -|-> DBDestination
CSVSource -|
```

In this example graph, RowTransformation2 has two inputs: DBSource2 & CSVSource. Also, DBDestination has two inputs:
RowTransformation1 & RowTransformation2. The DBDestination will complete when data from all sources 
(DBSource1, DBSource2, CSVSource) was written into the data flow and arrived at the DBDestination. 

*Note*: When you want to merge you data of multiple source before any further processing, consider using the 
`MergeJoin`. If you want to split your data, you can use the `Multicast`. 
[Read more about these transformations here.](dataflow_transformations.md)

## Synchronous Execution

The easiest way to execute a dataflow is synchronous. That means that execution of your program is paused
until all data was read from sources and written into all destinations. Using the synchronous execution also makes
debugging a lot easier, as you don't have to deal with async programming and the specialties of exception
handling with tasks.

Please note: In the background, the dataflow is always executed asynchronous! The underlying dataflow engine
is based on `Microsoft.TPL.Dataflow`. ETLBox will wrap this behavior into synchronous methods. 

### Example sync execution

```C#
//Prepare the flow
DBSource source = new DBSource("SourceTable");
RowTransformation rowTrans = new RowTransformation( row => row );
DBDestination dest = new DBDestination("DestTable");
source.LinkTo(row);

//Execute the source 
source.Execute();

//Wait for the destination
dest.Wait(); 
```

The Execute() method on the source will block execution until data is read from the source and posted into the dataflow.

The Wait() method on the destination will block execution until all data arrived at the destination. Under the hood,
this method will call the Wait() method of the Task from the underlying dataflow.

## Asynchronous execution

If you are familiar with async programming, you can also execute the dataflow asynchronous. This means that
execution of your program will continue while the data is read from the source and written into the destinations 
in separate task(s) in the background. 

### Example async execution

```C#
DBSource source = new DBSource("SourceTable");
RowTransformation rowTrans = new RowTransformation( row => row );
DBDestination dest = new DBDestination("DestTable");

source.LinkTo(row).LinkTo(dest);

Task sourceTask = source.ExecuteAsync();
Task destTask = dest.Completion;
try
{
    sourceTask.Wait();
    destTask.Wait();
} catch (Exception e)
{
    throw e.InnerException;
}
```

The `ExecuteAsync()` method will return a Task which completes when all data is read from the source and posted in the dataflow.
The `Completion` property will return a Task which completes when all data has arrived at the destination.

