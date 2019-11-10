# Overview File source and destination

## CSVSource

A CSV source simple reads data from a CSV file. Under the hood is the 3rd party library `CSVHelper`. There are several configuration options for the Reader. 
The default output data type of the CSVReader is a string array. You can either use a `RowTransformation` to transform it into the data type you need, or use
the generic implementation CSVSource.

```C#
//Returns string[] as output type for other compoments
CSVSource source = new CSVSource("Demo.csv") {
    Delimiter = ";",
    SourceCommentRows = 2
}
```

```C#
CSVSource<CSVData> source = new CSVSource<CSVData>("Demo.csv");
```

## CSVDestination

A csv destination will create a file containing the data in the desired CSV format. It is based on the 3rd party library `CSVHelper`.

There is a generic and non-generic class avaiable. The generic approach will create a csv file including a header column - the header name is derived 
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

CSVDestination<MySimpleRow> dest = new CSVDestination<MySimpleRow>("./SimpleWithObject.csv");
```

will create a .csv file like this

```
Header1,Header2
1,Test1
2,Test2
3,Test3
```

### CSV Configuration

The `CSVDestination` and the `CSVSource` does have a property `Configuration` which allows you to set e.g. the delimiter for the file. 

```C#
CSVSource source = new CSVSource("res/CSVSource/OneColumn.csv")
{
    Configuration = new CsvHelper.Configuration.Configuration() { Delimiter = ";" }
};
```

## JsonSource

Let's consider we have a json file like this as our data input:

```
[
  {
    "Col1": 1,
    "Col2": "Test1"    
  },
  {
    "Col1": 2,
    "Col2": "Test2"    
  }
]
```

This can be read into a dataflow using the `JsonSource` and the following code:

```C#
public class MySimpleRow
{
    public int Col1 { get; set; }
    public string Col2 { get; set; }
}

JsonSource<MySimpleRow> source = new JsonSource<MySimpleRow>("file.json");
```

## JsonDestination

To get your data outputted as json, you can use the `JSonDestination`:

```C#
JsonDestination<MySimpleRow> dest = new JsonDestination<MySimpleRow>("file.json");
```

## ExcelSource

An Excel source reads data from a xls or xlsx file. It uses the 3rd party library `ExcelDataReader`. 
By default the excel reader will try to read all data in the file. You can specify a sheet name and a range 
to restrict this behaviour. Additionally, you have to use the Attribute `ExcelColumn` to define the column index
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

