# Flat files and other sources and destinations

## CsvSource

A CSV source simple reads data from a CSV file. Under the hood is the 3rd party library `CSVHelper`. There are several configuration options for the Reader. 
The default output data type of the CSVReader is a string array. You can either use a `RowTransformation` to transform it into the data type you need, or use
the generic implementation CsvSource.

```C#
//Returns string[] as output type for other components
CsvSource source = new CsvSource("Demo.csv") {
    Delimiter = ";",
    SourceCommentRows = 2
}
```

```C#
CsvSource<CSVData> source = new CsvSource<CSVData>("Demo.csv");
```

## CsvDestination

A CSV destination will create a file containing the data in the desired CSV format. It is based on the 3rd party library `CSVHelper`.

There is a generic and non-generic class available. The generic approach will create a csv file including a header column - the header name is derived 
from the property names or the CSVHelper attributes.

E.g.

```C#
 public class MySimpleRow
{    
    [Name("Header1")]
    [Index(1)]
    public int Col1 { get; set; }
    [Name("Header2")]
    [Index(2)]
    public string Col2 { get; set; }
}

CsvDestination<MySimpleRow> dest = new CsvDestination<MySimpleRow>("./SimpleWithObject.csv");
```

will create a .csv file like this

```
Header1,Header2
1,Test1
2,Test2
3,Test3
```

### CSV Configuration

The `CsvDestination` and the `CsvSource` does have a property `Configuration` which allows you to set e.g. the delimiter for the file. 

```C#
CsvSource source = new CsvSource("res/CsvSource/OneColumn.csv")
{
    Configuration = new CsvHelper.Configuration.Configuration() { Delimiter = ";" }
};
```

## Xml

### XmlSource

The xml source let you read data from a xml source.
Let's assume your xml file looks like this:

```xml
<?xml version="1.0" encoding="utf-8"?>
<Root>
    <MySimpleRow Col1="1">
        <Col2>Test1</Col2>
    </MySimpleRow>
    <MySimpleRow Col1="2">
        <Col2>Test2</Col2>
    </MySimpleRow>
</Root>
```

Xml parsing is based on XmlSerializer. You can define your source and your object like this, and  make use of the default xml attribute annotations to
influence the XmlSerializer.

```C#
[XmlRoot("MySimpleRow")]
public class MyRow
{
    [XmlAttribute]
    public int Col1 { get; set; }
    public string Col2 { get; set; }
}

  XmlSource<MyRow> source = new XmlSource<MyRow>("source.xml", ResourceType.File);
```

### XmlDestination

The xml destination will you the same XmlSerializer to deserialize the data and write them into an xml file.

```C#
XmlDestination<MyRow> dest = new XmlDestination<MyRow>("dest.xml", ResourceType.File);
```

## ExcelSource

An Excel source reads data from a xls or xlsx file. 
[It uses the 3rd party library `ExcelDataReader`](https://github.com/ExcelDataReader/ExcelDataReader). 
By default the excel reader will try to read all data in the file. You can specify a sheet name and a range 
to restrict this behavior. Additionally, you have to use the Attribute `ExcelColumn` to define the column index
for each property. The first column would be 0, the 2nd column 1, ...

Usage example:

```C#

public class ExcelData {
    [ExcelColumn(0)]
    public string Col1 { get; set; }
    [ExcelColumn(1)]
    public int Col2 { get; set; }
}

ExcelSource<ExcelData> source = new ExcelSource<ExcelData>("src/DataFlow/ExcelDataFile.xlsx") {
    Range = new ExcelRange(2, 4, 5, 9),
    SheetName = "Sheet2"
};
```
The ExcelRange must not define the full range. It is sufficient if you just set the starting coordinates. The end of the
data can be automatically determined from the underlying ExcelDataReader.

The ExcelSource has a property `IgnoreBlankRows`. This can be set to true, and all rows which cells are completely empty
are ignored when reading data from your source. 

## Other Sources and Destinations

### Custom integration

#### Custom Source

A custom source can generate any type of of output you describe in a Function. There is a function that describe how the records
are generated and a function that returns true if you reached the end of your data. 

```C#
List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
int _readIndex = 0;
Func<MySimpleRow> ReadData = () =>
{
    var result = new MySimpleRow()
    {
        Col1 = _readIndex + 1,
        Col2 = Data[_readIndex]
    };
    _readIndex++;
    return result;
};

Func<bool> EndOfData = () => _readIndex >= Data.Count;

//Act
CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
```

#### Custom Destination

A custom destination calls the given action for every received record in the destination.

```C#
CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
    row => {
        SqlTask.ExecuteNonQuery(Connection, "Insert row",
            $"INSERT INTO dbo.CustomDestination VALUES({row.Col1},'{row.Col2}')");
    }
);
```

### Integrate from memory

#### Memory Source

A Memory source is a simple source components that accepts a list. Use this component
within your dataflow if you already have you collection or records available in memory.
When you execute the flow, the memory destination will iterate through the list and 
asynchronously post record by record into the flow.

Example code:

```C#
 TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("MemoryDestination");
MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(SqlConnection, "MemoryDestination");
       
source.Data = new List<MySimpleRow>()
{
    new MySimpleRow() { Col1 = 1, Col2 = "Test1" },
    new MySimpleRow() { Col1 = 2, Col2 = "Test2" },
    new MySimpleRow() { Col1 = 3, Col2 = "Test3" }
};
source.LinkTo(dest);
source.Execute();
dest.Wait();
```

#### MemoryDestination

A memory destination is a component that store the incoming data within a [BlockingCollection](https://docs.microsoft.com/de-de/dotnet/api/system.collections.concurrent.blockingcollection-1?view=netframework-4.8).
It stores the data within the `Data` property.
Data can be read from this collection as soon as it arrives. 

Example:

```C#
DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "MemoryDestinationSource");
MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

source.LinkTo(dest);
Task st = source.ExecuteAsync();
Task dt = dest.Completion();

// data is accessible in dest.Data
```

When starting the data flow asynchronous, you should wait until the tasks complete. The source task will complete when 
all data was posted into the data flow, and the destination task completes when all data has arrived in the destination. 

## VoidDestination

A `VoidDestination` is a destination where all incoming data is ignored. This can be helpful if you work with Predicates.
For more details [see the article about Predicates](dataflow_linking_execution.md). 