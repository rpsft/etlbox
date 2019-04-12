# Overview Data Flow Sources

## DBSource

The DBSource is the most common data source for a data flow. It basically connects to a database via ADO.NET and executes a SELECT-statement to start reading the data. While ADO.NET is reading from the source, data is simutaneously posted into the dataflow pipe.
To initialize a DBSource, you can either hand over a `TableDefinition`, a SQL-statement or a tablename. 
The DBSource needs to be defined with a POCO that matches the data types of the data. 
By default, the mapping of column names to properties is resolved by the property name itself. E.g. a column named Value1 
would stored in the property with the same name. If you use the `ColumnMap` attribute, you can add what column name will be mapped 
to the property. If there is no match, the column will be ignored.

Usage example:

```C#
public class MySimpleRow {
    [ColumnMap("Value1")]
    public string Col1 { get; set; }
    public int Value2 { get; set; }
}

DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(
    $@"select Value1, Value2 from dbo.Test"
);
```

## DBDestination

Like the `DBSource`, the `DBDestination` is the common component for sending data into a database. It is initalized with a table name or a `TableDefinition`.

Usage example:

```C#
 DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("test.Destination");
 ```

### Sql Server Connections

The `DBSource` and `DBDestination` can be used to connect via ADO.NET to a sql server. Use the `ConnectionString` object and a `SqlConnectionManger` to create a regular ADO.NET connection. 

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;"));
```

### SMO Connection Manager

The `SMOConnectionManager` uses Sql Server Managed Objects to connect to a Sql Server. It allow the use of the GO keyword within your SQL to separate batches. It can be used with a `ConnectionString`.

```C#
ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;"));
```

### ODBC Connections
The `DBSource` and `DBDestination` also works with ODBC connection. Use an `OdbcConnectionManager` and an `OdbcConnectionString`. You will still use the underlying ADO.NET, but it allows you not only to connect to SQL Server but to all databases that support ODBC. 
  
```C#
  OdbcConnectionManager con = new OdbcConnectionManager(new OdbcConnectionString("Driver={SQL Server};Server=.;Database=ETLBox;Trusted_Connection=Yes;"));
```

*Warning*: ODBC does not support bulk inserts. The `DBDestination` will do a bulk insert by creating a sql insert statement that
has multiple values: INSERT INTO (..) VALUES (..),(..),(..)

*Warning*: Not all Control Flow Tasks may be supported when using ODBC connections!

### Access DB Connections

The ODBC connection to Microsoft Access databases have some more restrictions. ETLBox is based .NET Core and will only
support 64bit ODBC connections. You need also have Microsoft Access 64 bit installed. (The corresponding 64bit ODBC driver for Access can be download 
Microsoft: [Microsoft Access Database Engine 2010 Redistributable](https://www.microsoft.com/en-us/download/details.aspx?id=13255)
To create a connection to an Access Database, use the `AccessOdbcConnectionManager` and an `OdbcConnectionString`.

```C#
ControlFlow.CurrentDbConnection = new AccessOdbcConnectionManager(new OdbcConnectionString("Driver={Microsoft Access Driver (*.mdb, *.accdb)}DBQ=C:\DB\Test.mdb")) {
    AlwaysUseSameConnection = false
};
```

*Warning*: The `DBDestination` will do a bulk insert by creating a sql statement using a sql query that Access understands. The number of rows per batch is very limited - if it too high, you will the error message 'Query to complex'. Try to reduce the batch size to solve this.

*Note*: Please note that the AccessOdbcConnectionManager will create a "temporary" dummy table containing one record in your database when doing the bulk insert. After completion it will delete the table again. This was necessary to simulate a bulk insert with Access-like Sql. 

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

