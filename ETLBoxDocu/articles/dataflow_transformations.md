# Overview Data Flwo Transformations

Transformations always have at least one input and one output. Inputs can be connected either to other transformations or sources, and the output can also connect to other transformations
or to destinations. 
The purpose of a transformation component is to take the data from its input(s) and post the transformed data to its outputs. This is done on a row-by-row basis.
As soon as there is any data in the input, the transformation will start and post the result to the output. 

### Buffering

Every transformation will come with an input. If the components connected to the input post data faster than the transformation
can process it, the buffer will hold this data until the transformation can continue with the next item. This allows a source to read as fast as possible,
allowing the already read data to be buffered in the memory - so the transformation will always have some data ready to process.

### Non-Blocking and Blocking transformations

Transformation can be either blocking or non-blocking. 

Non-Blocking transformations will start to process data as soon as it finds something in its input buffer. 
In the moment where it discovers data in there, it will  start to transform it and send the data to registered output components. 

Blocking transformations will stop the data processing for the whole pipe - the input buffer will wait until all data has reached the input. This means it will wait until
all sources in the pipe connected to the transformation have read all data from their source, and all transformations before have processed the incoming data. 
When all data was read from the connected sources and transformations further down the pipe, the blocking transformation will start the transformation. In a transformation
of a blocking transformation, you will therefore have access to all data buffered within the memory. For instance, the sort component is a blocking transformation. 
It will wait until all data has reached the transformation block - then it will sort it and post the sorted data to its output. 


## Non blocking tranformations

### RowTransformations

The RowTransformation is the simplest but most powerful transformation in ETLBox. The generic transformation has two types 
- the type of the input data and the type of the output data. When creating a RowTransformation, you pass a delegate
describing how each record in the dataflow is transformed. Here you can add any C# code that you like. 

The RowTransformation is a non blocking transformation, so it won't use up much memory even for high amoutns of data.

Here is an example that convert a string array into a `MySimpleRow` object.

```C#
public class MySimpleRow
{
    public int Col1 { get; set; }
    public string Col2 { get; set; }
}

RowTransformation&lt;string[], MySimpleRow&gt; trans = new RowTransformation&lt;string[], MySimpleRow&gt;(
    csvdata =>
    {
        return new MySimpleRow()
        {
            Col1 = int.Parse(csvdata[0]),
            Col2 = csvdata[1]
        };
});
```

### Lookup

The lookup is a row transformation, but before it starts processing any rows it will load all data from the given LookupSource into memory 
and will make it accessable as a List object.
Though the lookup is non-blocking, it will take as much memory as the lookup table needs to be loaded fully into memory. 

Here is an example:

```C#
DBSource<MyLookupRow> lookupSource = new DBSource<MyLookupRow>(connection, "Lookup");
List<MyLookupRow> LookupTableData = new List<MyLookupRow>();
Lookup<MyInputDataRow, MyOutputDataRow, MyLookupRow> lookup = new Lookup<MyInputDataRow, MyOutputDataRow, MyLookupRow>(
    row =>
    {
        MyOutputDataRow output = new MyOutputDataRow()
        {
            Col1 = row.Col1,
            Col2 = row.Col2,
            Col3 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue1).FirstOrDefault(),
            Col4 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue2).FirstOrDefault(),
        };
        return output;
    }
    , lookupSource
    , LookupTableData
);
```

### Multicast

A multicast split the input into two or more outputs. So basically you duplicate you data.

An example would look like this:

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(Connection, "Source");
DBDestination<MySimpleRow> dest1 = new DBDestination<MySimpleRow>(Connection, "Destination1");
DBDestination<MySimpleRow> dest2 = new DBDestination<MySimpleRow>(Connection, "Destination2");
DBDestination<MySimpleRow> dest3 = new DBDestination<MySimpleRow>(Connection, "Destination3");

Multicast<MySimpleRow> multicast = new Multicast<MySimpleRow>();
source.LinkTo(multicast);
multicast.LinkTo(dest1);
multicast.LinkTo(dest2);
multicast.LinkTo(dest3);
source.Execute();
dest1.Wait();
dest2.Wait();
dest3.Wait();
```

### MergeJoin

A merge join combines two inputs into one output. A function describes how the two inputs are combined into one output. The type of the 
output and the inputs can be different, as long as you handle it in the join function.
MergeJoin is a non blocking transformation. 

Example: 

```
DBSource<MySimpleRow> source1 = new DBSource<MySimpleRow>(Connection, "MergeJoinSource1");
DBSource<MySimpleRow> source2 = new DBSource<MySimpleRow>(Connection, "MergeJoinSource2");
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(Connection, "MergeJoinDestination");

//Act
MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow> join = new MergeJoin<MySimpleRow, MySimpleRow, MySimpleRow>(
    (inputRow1, inputRow2) => {
        inputRow1.Col1 += inputRow2.Col1;
        inputRow1.Col2 += inputRow2.Col2;
        return inputRow1;
    });
source1.LinkTo(join.Target1);
source2.LinkTo(join.Target2);
join.LinkTo(dest);
source1.Execute();
source2.Execute();
dest.Wait();
```

## Blocking Transformations

## BlockTransformation

A BlockTransformation waits until all data is received at the BlockTranformation - then it will be available in a List object and you can do modifications
on your whole data set. Keep in mind that this tranformation will need as much memory as the amount of data you loaded. 

```C#
BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(
    inputData => {
        inputData.RemoveRange(1, 2);
        inputData.Add(new MySimpleRow() { Col1 = 4, Col2 = "Test4" });
        return inputData;
    });
```

## Sort

A sort will wait for all data to arrive and then sort the data based on the given sort method. 

```C#
Comparison<MySimpleRow> comp = new Comparison<MySimpleRow>(
        (x, y) => y.Col1 - x.Col1
    );
Sort<MySimpleRow> block = new Sort<MySimpleRow>(comp);
```

