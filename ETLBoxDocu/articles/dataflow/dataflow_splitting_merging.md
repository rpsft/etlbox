# Splitting and merging data

## Splitting data

In some of your data flow you may want to split the data and have it processed differently in the further flow.
E.g. your data comes from one source and you want parts of it written into one destination and parts of it
written into another. Or you like to split up data based on some conditions. For this purpose you can use the Multicast

## Multicast

The `Multicast` is a component which basically duplicates your data. It has one input and two or more outputs.
(Technically, it could also be used with only one output, but then it wouldn't do much.)

The following code demonstrate a simple example where data would be duplicated and copied into two destinations - 
a database table and a Json file. 

```C#
var source = new CSVSource("test.csv");

var multicast = new Multicast();
var destination1 = new JsonDestination("test.json");
var destination2 = new DBDestination("TestTable");

source.LinkTo(multicast);
multicast.LinkTo(destination1);
multicast.LinkTo(destination2);
```

If you want to split data, you can use [Predicates](dataflow_predicates.md).
Predicates allow you to let only certain data pass. 
E.g. the following code would only copy data into Table1 where the first column is greater 0, the rest will be 
copied into Table2.

```C#
var source = new CSVSource("test.csv");

var multicast = new Multicast();
var destination1 = new DBDestination("Table1");
var destination2 = new DBDestination("Table2");

source.LinkTo(multicast);
multicast.LinkTo(destination1, row => row[0] > 0);
multicast.LinkTo(destination2, row => row[0] < 0);
```

Please note: Make sure when using predicate that always all rows arrive at a destination. Use a `VoidDestination`
for records that you don't want to keep. See more about this in the [article about Predicates](dataflow_predicates.md).

## Merging data

If you want to merge data in your dataflow, you can use the `MergeJoin`.

The MergeJoin accepts two inputs and has one output. E.g. you can link two sources with the MergeJoin, define 
a method how to combine these records and produce a new merged output. The data types of inputs and output can be
different.

```C#
DBSource<MyInputRowType1> source1 = new DBSource<MyInputRowType1>(Connection, "MergeJoinSource1");
DBSource<MyInputRowType2> source2 = new DBSource<MyInputRowType2>(Connection, "MergeJoinSource2");
DBDestination<MyOutputRowType> dest = new DBDestination<MyOutputRowType>(Connection, "MergeJoinDestination");

//Act
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