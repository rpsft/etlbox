# Example: Multicast to split data

## Purpose

The following examples shows how to use a Multicast to split data. We receive data from one flat file
and we want to split up the data into two tables, each containing only a part of the data.

## Creating the flat file

For this example we have a flat file containing a small example data set

```CSV
Value1;Value2;Value3;Value4
one;two;three;four
five;six;seven;eight
```

## Creating the POCOs

```C#
internal class TestPoco
{
    public string Value1 { get; set; }
    public string Value2 { get; set; }
    public string Value3 { get; set; }
    public string Value4 { get; set; }
}

internal class TestEntity1
{
    public string Col1 { get; set; }
    public string Col3 { get; set; }
}

internal class TestEntity2
{
    public string Col2 { get; set; }
    public string Col4 { get; set; }
}
```

## Creating the destination tables

Now we create the destination tables.

```C#
    private TableDefinition CreateTable(string tablename)
    {
        var def = new TableDefinition(tablename, new List<TableColumn>
        {
        new TableColumn("Col1", "nvarchar(100)", true),
        new TableColumn("Col2", "nvarchar(100)", true),
        new TableColumn("Col3", "nvarchar(100)", true),
        new TableColumn("Col4", "nvarchar(100)", true)
        });
        def.CreateTable();
        return def;
    }
```

## Creating the dataflow

Now we create a dataflow that reads from the source and splits the data into Table1 and Table2.

```C#
var tableDestination1 = this.CreateTable("test.Table1");
var tableDestination2 = this.CreateTable("test.Table2");

var row1 = new RowTransformation<TestPoco, TestEntity1>(input => {
    return new TestEntity1
    {
        Col1 = input.Value1,
        Col3 = input.Value3
    };
});
var row2 = new RowTransformation<TestPoco, TestEntity2>(input => {
    return new TestEntity2
    {
        Col2 = input.Value2,
        Col4 = input.Value4
    };
});

var source = new CsvSource<TestPoco>("src/DataFlowExamples/Issue5.csv") {
    Configuration = new CsvHelper.Configuration.Configuration() { Delimiter = ";" }
};
var multicast = new Multicast<TestPoco>();
var destination1 = new DbDestination<TestEntity1>("test.Table1");
var destination2 = new DbDestination<TestEntity2>("test.Table2");

source.LinkTo(multicast);
multicast.LinkTo(row1);
multicast.LinkTo(row2);

row1.LinkTo(destination1);
row2.LinkTo(destination2);

source.Execute();
destination1.Wait();
destination2.Wait();
```