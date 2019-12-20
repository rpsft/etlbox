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
there is a `VoidDestination` in ETLBox. Use this destination for all records for that you don't wish any futher processing. 

```C#
VoidDestination voidDest = new VoidDestination(); 
source.LinkTo(dest, row => row.Value > 0);
souce.Link(voidDest, row => row.Value <= 0);
```

#### Implicit use of VoidDestination

You don't have to define the `VoidDestinatin` explicitly. Assuming that we have a Database Source 
that we want to link to a database destination. But we only want to let data pass throught where the 
a column is greater than 0. The rest we want to ignore. Normally, we would need to link the data twice like in 
the example above. But there is a short way to write it: 

```C#
source.LinkTo(dest, row => row.Value > 0,  row => row.Value <= 0);
```

Internally, this will create a `VoidDestination` when linking the components, but you don't have to deal with anymore.
At the end, only records where the Value column is greate 0 will be written into the destination.


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


