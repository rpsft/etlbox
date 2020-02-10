# Transformations

Transformations always have at least one input and one output. Inputs can be connected either to other transformations or sources, and the output can also connect to other transformations
or to destinations. 
The purpose of a transformation component is to take the data from its input(s) and post the transformed data to its outputs. This is done on a row-by-row basis.
As soon as there is any data in the input, the transformation will start and post the result to the output. 

## Buffering

Every transformation will come with an input. If the components connected to the input post data faster than the transformation
can process it, the buffer will hold this data until the transformation can continue with the next item. This allows a source to read as fast as possible,
allowing the already read data to be buffered in the memory - so the transformation will always have some data ready to process.

## Non-Blocking and Blocking transformations

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

The RowTransformation is a non blocking transformation, so it won't use up much memory even for high amounts of data.

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

### LookupTransformation

The lookup is a row transformation, but before it starts processing any rows it will load all data from the given LookupSource into memory 
and will make it accessible as a List object.
Though the lookup is non-blocking, it will take as much memory as the lookup table needs to be loaded fully into memory. 

Here is an example:

```C#
DbSource<MyLookupRow> lookupSource = new DbSource<MyLookupRow>(connection, "Lookup");
List<MyLookupRow> LookupTableData = new List<MyLookupRow>();
LookupTransformation<MyInputDataRow, MyLookupRow> lookup = new Lookup<MyInputDataRow, MyLookupRow>(
    lookupSource,
    row =>
    {
        Col1 = row.Col1,
        Col2 = row.Col2,
        Col3 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue1).FirstOrDefault(),
        Col4 = LookupTableData.Where(ld => ld.Key == row.Col1).Select(ld => ld.LookupValue2).FirstOrDefault(),
        return row;
    }
    , LookupTableData
);
```

### Splitting data

In some of your data flow you may want to split the data and have it processed differently in the further flow.
E.g. your data comes from one source and you want parts of it written into one destination and parts of it
written into another. Or you like to split up data based on some conditions. For this purpose you can use the Multicast

#### Multicast

The `Multicast` is a component which basically duplicates your data. It has one input and two or more outputs.
(Technically, it could also be used with only one output, but then it wouldn't do much.)
Multicast is a non-blocking operation. 

The following code demonstrate a simple example where data would be duplicated and copied into two destinations - 
a database table and a Json file. 

```C#
var source = new CsvSource("test.csv");

var multicast = new Multicast();
var destination1 = new JsonDestination("test.json");
var destination2 = new DbDestination("TestTable");

source.LinkTo(multicast);
multicast.LinkTo(destination1);
multicast.LinkTo(destination2);
```

If you want to split data, you can use Predicates.
Predicates allow you to let only certain data pass. 
E.g. the following code would only copy data into Table1 where the first column is greater 0, the rest will be 
copied into Table2.

```C#
var source = new CsvSource("test.csv");

var multicast = new Multicast();
var destination1 = new DbDestination("Table1");
var destination2 = new DbDestination("Table2");

source.LinkTo(multicast);
multicast.LinkTo(destination1, row => row[0] > 0);
multicast.LinkTo(destination2, row => row[0] < 0);
```

Please note: Make sure when using predicate that always all rows arrive at a destination. Use a `VoidDestination`
for records that you don't want to keep. See more about this in the [article about Predicates](dataflow_linking_execution.md).

### Merging data

If you want to merge data in your dataflow, you can use the `MergeJoin`. This basically joins the outcome
 of two sources or transformations into one data record.

#### MergeJoin

The MergeJoin accepts two inputs and has one output. A function describes how the two inputs are combined into one output. 
E.g. you can link two sources with the MergeJoin, define 
a method how to combine these records and produce a new merged output. The data type of the 
output and the inputs can be different, as long as you handle it in the join function.
MergeJoin is a non blocking transformation. 

```C#
DbSource<MyInputRowType1> source1 = new DbSource<MyInputRowType1>(Connection, "MergeJoinSource1");
DbSource<MyInputRowType2> source2 = new DbSource<MyInputRowType2>(Connection, "MergeJoinSource2");
DbDestination<MyOutputRowType> dest = new DbDestination<MyOutputRowType>(Connection, "MergeJoinDestination");

MergeJoin<MyInputRowType1, MyInputRowType2, MyOutputRowType> join = new MergeJoin<MyInputRowType1, MyInputRowType2, MyOutputRowType>(
    (inputRow1, inputRow2) => {
        return new MyOutputRowType() {
            Value = inputRow1.Value + inputRow2.Value
        };
    });
source1.LinkTo(join.Target1);
source2.LinkTo(join.Target2);
join.LinkTo(dest);
```

## Blocking Transformations

### BlockTransformation

A BlockTransformation waits until all data is received at the BlockTranformation - then it will be available in a List object and you can do modifications
on your whole data set. Keep in mind that this transformation will need as much memory as the amount of data you loaded. 

```C#
BlockTransformation<MySimpleRow> block = new BlockTransformation<MySimpleRow>(
    inputData => {
        inputData.RemoveRange(1, 2);
        inputData.Add(new MySimpleRow() { Col1 = 4, Col2 = "Test4" });
        return inputData;
    });
```

### Sort

A sort will wait for all data to arrive and then sort the data based on the given sort method. 

```C#
Comparison<MySimpleRow> comp = new Comparison<MySimpleRow>(
        (x, y) => y.Col1 - x.Col1
    );
Sort<MySimpleRow> block = new Sort<MySimpleRow>(comp);
```

