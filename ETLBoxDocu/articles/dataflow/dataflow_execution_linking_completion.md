# Exceution, Linking and Completion

## Linking components

Before you can execute a data flow, you need to link your source, transformation and destinations.
The linking is quite easy - every source component and every transformation offers a LinkTo() method.
This method accepts a target, which either is another transformation or a destination. 

Example of Linking a `DBSource` to a `RowTransformation` and the to a `DBDestination`.

```
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

This notation can be used most of the time - please note that it won't work with `Multicast` or `MergeJoin`. 

## Synchronous Execution

The easiest way to execute a dataflow is synchrounous. That means that execution of your program is paused
until all data was read from sources and written into all destinations. Using the synchronous execution also makes
debugging a lot easier, as you don't have to deal with async programming and the specialities of exception
handling with tasks.

Please note: In the background, the Dataflow is always executed asynchrounous! The underlying dataflow engine
is based on `Microsoft.TPL.Dataflow`. ETLBox will wrap this behaviour into synchronous methods. 

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
in seperate task(s) in the background. 

### Example async execution

```
DBSource source = new DBSource("SourceTable");
RowTransformation rowTrans = new RowTransformation( row => row );
DBDestination dest = new DBDestination("DestTable");

source.LinkTo(row).LinkTo(dest);

Task sourceTask = source.ExecuteAsync();
Task destTask = dest.Completion();
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
The `Completion()` method will return a Task which completes when all data has arrived at the destination.


