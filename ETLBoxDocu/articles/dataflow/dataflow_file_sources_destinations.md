# Flat files and other sources and destinations

## CsvSource

A CcsvSource simple reads data from a CSV file. 
It is based on the [library CsvHelper created by Josh Close](https://joshclose.github.io/CsvHelper/).
In the following examples, you will learn how to configure the CsvReader to your needs.
See the documentation of CsvHelper to learn more about the configuration options for the CsvReader itself.

Let's start with a simple example:

```C#
CsvSource source = new CsvSource("Demo.csv");
source.Configuration.Delimiter = ";";
source.Configuration.IgnoreBlankLines = true;
```

This will creata a source component that reads the data from a "Demo.csv" file. This file could look like this:

```csv
Row_Nr;Value
1;Test1
2;Test2
```

There are several configuration options for the Reader that you can set in the Configuration property. Learn more
about these options [in the CsvHelper.Configuration api documentation](https://joshclose.github.io/CsvHelper/api/CsvHelper.Configuration/Configuration/).
The default output data type of the CsvSource is an ExpandoObject. This is a dynamic object which will contain a property 
for each column in your csv file. The first row of your file is supposed to be a header record (unless you use the SkipRows property to define how many
rows needs to be skipped before your header starts). The header will define the property names of the ExpandoObject.

You can now use a `RowTransformation` to transform it into the data type you need, or just stick with the ExpandoObject. (All other components
in ETLBox will also support this).

This is an example to transform the dynamic object into a regular .NET object:

```C#
 CsvSource<ExpandoObject> source = new CsvSource<ExpandoObject>("Demo.csv");
RowTransformation<ExpandoObject,MyDataObject> trans = new RowTransformation<ExpandoObject,MyDataObject>(
    csvdata =>
    {
        dynamic csvrow = csvdata as ExpandoObject;
        MyDataObject myData = new MyDataObject() {
            myData.Id = csvRow.Row_Nr;
            myData.Value = csvRow.Value;
        };
        return myData;
    });
```

#### Using object types

Of course you can  use your data object as type for the CsvSource. The following code would directly read the data from the csv file 
into the right object type.

```C#
public class MyCsvData {
    public int Row_Nr { get; set; }
    public string Value { get; set; }
}
CsvSource<MyCsvData> source = new CsvSource<MyCsvData>("Demo.csv");
```

ETLBox will find the right property by the equivalent header column in your file. Therefore, the order of the columns doesn't matter, as long
as the column has an equivalent header. If the header name is different, you can use attributes or a ClassMap to find the right column.
Here is an example for using the Name and index attribute:

```C#
public class MyCsvData {
    [Name("Row_nr")]
    public int Id { get; set; }
    [Index(1)]
    public string Text { get;set;}
}
CsvSource<MyCsvData> source = new CsvSource<MyCsvData>("Demo.csv");
```

See the full documentation [about CsvHelepr attributes here](https://joshclose.github.io/CsvHelper/examples/configuration/attributes) or 
read more [about class maps](https://joshclose.github.io/CsvHelper/examples/configuration).

#### Using arrays

Sometimes it can be easier to use a string array (or object array) to read from a csv file, e.g. if your Csv file doesn't have a header.
ETLBox will support arrays as well - just define your CsvSource like this

```C#
CsvSource<string[]> source = new CsvSource<string[]>("Demo.csv");
source.Configuration.HasHeaderRecord = false;
```

## CsvDestination

A CSV destination will create a file containing the data in the desired CSV format. 
Like the CsvSource it is based on the [library CsvHelper created by Josh Close](https://joshclose.github.io/CsvHelper/). 

The CsvDestination will work with the dynamic (ExpandoObject) as well as with regular object or arrays. 
Here is an example how you can use a classic object to write data into a Csv file:

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

If you use the ExpandoObject, the header names will be derived from the property names. In most cases, this will work as expected. 
If you use an array, e.g. `CsvDestination<string[]>`, you won't get a header column.


## Xml

### XmlSource

The xml source let you read data from a xml source. It will use  `System.Xml` under the hood. 

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

Xml reading is based on the Micrsoft XmlSerializer (using System.Xml.Serialization). You can make use of the default xml attribute 
annotations to influence how data is read by the XmlSerializer. For the example xml above, the following code could read the xml file:

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

#### Using dynamic objects

XmlSource does also support the dynamic ExpanoObject. If you want to use it, you can define an ElementName that contains the data you acutally
want to parse - as you normally are not interested in your root element. ETLBox then will look for this Element and parse every occurence of
it into an ExpandoObject and send it into the connected components. 

Here is an example. If your xml looks like this:

```xml
<?xml version="1.0" encoding="utf-8"?>
<Root>
    <MySimpleRow>
        <Column1>1</Column1>
        <Column2>Test1</Column2>
    </MySimpleRow>
    <MySimpleRow>
        <Column1>2</Column1>
        <Column2>Test2</Column2>
    </MySimpleRow>
<Root>
```

You can parse the two elements <MySimpleRow> with the follwoing code:

```C#
XmlSource source = new XmlSource("demo2.xml", ResourceType.File)
{
    ElementName = "MySimpleRow"
};
```

### XmlDestination

The xml destination will use the same XmlSerializer to serialize the data and write them into an xml file.

Here is an example how to influence the XmlSerializer using attributes:

```C#
[XmlRoot("MySimpleRow")]
public class MyRow
{
    [XmlAttribute]
    public int Col1 { get; set; }
    [XmlAttribute]
    public string Col2 { get; set; }
}

XmlDestination<MyRow> dest = new XmlDestination<MyRow>("dest.xml", ResourceType.File);
```

Could create an output that looks like this:

```xml
<?xml version="1.0" encoding="utf-8"?>
<Root>
  <MySimpleRow Col1="1" Col2="Test1" />
  <MySimpleRow Col1="2" />
</Root>
```

## Resource Type and Web Requests

You may have noticed that both sources and destinations (Xml and Csv) are use with the `ResourceType.File` option.
This is default for CsvSource/CsvDestination, but not for the XmlSource/XmlDestination. The other option
is ResourceType.Http - and allows you to read data from a web service. Instead of a filename just provide
and Url. Furthermore, the components also have a `[HttpClient](https://docs.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=netframework-4.8)` property that can be used to configure the Http request 
(e.g. to add authentication.)
As Csv and Xml is not so commonly used anymore, you can read more about querying data from web services in 
the article [Json and webservice integration](dataflow_web_services.md).

## ExcelSource

An Excel source reads data from a xls or xlsx file. 
[It is based the 3rd party library `ExcelDataReader`](https://github.com/ExcelDataReader/ExcelDataReader). 
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

### CustomSource

A custom source can generate any type of  output you need. 
It will accept tow function: One function that generates the your output, and another one that return true if you reached the end of your data. 

Let's look at a simple example. Assuming we have a list of strings, and we want to return these string wrapped into an object data for our source.

First we define an object

```C#
public class MyRow {
    public int Id { get; set; }
    public string Value { get; set; }
}

List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
int _readIndex = 0;

CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(
    () => {
        return new MyRow()
        {
            Id = _readIndex++,
            Value = Data[_readIndex]
        };
    }, 
    () => _readIndex >= Data.Count);
```

CustomSource also works with dynamic ExpandoObject and arrays. 

#### Custom Destination

The use of a custom destination is even simpler - a custom destination 
just calls an action for every received record.

Here is an example:

```C#
CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
    row => {
        SqlTask.ExecuteNonQuery(Connection, "Insert row",
            $"INSERT INTO dbo.CustomDestination VALUES({row.Id},'{row.Value}')");
    }
);
```

### Memory Source

A Memory source is a simple source component that accepts a .NET list or enumerable. Use this component
within your dataflow if you already have a collection containing your data available in memory.
When you execute the flow, the memory destination will iterate through the list and 
asynchronously post record by record into the flow.

Example code:

```C#
MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
source.Data = new List<MySimpleRow>()
{
    new MySimpleRow() { Col1 = 1, Col2 = "Test1" },
    new MySimpleRow() { Col1 = 2, Col2 = "Test2" },
    new MySimpleRow() { Col1 = 3, Col2 = "Test3" }
};
```

### MemoryDestination

A memory destination is a component that store the incoming data within a [BlockingCollection](https://docs.microsoft.com/de-de/dotnet/api/system.collections.concurrent.blockingcollection-1?view=netframework-4.8).
You can access the received data within the `Data` property.
Data can be read from this collection as soon as it arrives. 

Example:

```C#
MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();
// data is accessible in dest.Data 
```

When starting the data flow asynchronous, you should wait until the tasks complete. The source task will complete when 
all data was posted into the data flow, and the destination task completes when all data has arrived in the destination. 
