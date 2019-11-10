# Predicates

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

## VoidDestination

Whenever you use predicates, sometime you end up with data which you don't want to write into a destination.
Unfortunately, your DataFlow won't finish until all rows where written into any destination block. That's why 
there is a `VoidDestination` in ETLBox. Use this destination for all records for that you don't wish any futher processing. 

```C#
VoidDestination voidDest = new VoidDestination(); 
source.LinkTo(dest, row => row.Value > 0);
souce.Link(voidDest, row => row.Value <= 0);
```


### Implicit use of VoidDestination

You don't have to define the `VoidDestinatin` explicitly. Assuming that we have a Database Source 
that we want to link to a database destination. But we only want to let data pass throught where the 
a column is greater than 0. The rest we want to ignore. Normally, we would need to link the data twice like in 
the example above. But there is a short way to write it: 

```C#
source.LinkTo(dest, row => row.Value > 0,  row => row.Value <= 0);
```

Internally, this will create a `VoidDestination` when linking the components, but you don't have to deal with anymore.
At the end, only records where the Value column is greate 0 will be written into the destination.
