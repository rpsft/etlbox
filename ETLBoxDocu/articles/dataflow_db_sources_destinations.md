# Overview Database source and destination

This overview will give you a description of all the sources and destination reading and writing from databases

## DBSource

The DBSource is the most common data source for a data flow. It basically connects to a database via ADO.NET and executes a SELECT-statement to start reading the data. 
While ADO.NET is reading from the source, data is simutaneously posted into the dataflow pipe.
To initialize a DBSource, you can either hand over a `TableDefinition`, a SQL-statement or a tablename. 
The DBSource also accepts a connection manager. If no connection manager is specified, the "default" connection manager is used 
(which is stored in `ControlFlow.CurrentDbConnection`)

### Generic approach

The DBSource can be defined with a POCO that matches the data types of the data. 
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

### Non generic approach

Instead of parsing your source data into a object, you can just read all your data in to string array. This is equivalent to `DBSource<string[]>`.
E.g.:

```
DBSource source = new DBSource($@"select Value1, Value2 from dbo.Test");
RowTransformation row = new RowTransformation( 
    row => {
        string value1 = row[0];
        string value2 = row[1];
        }
);
```

would have all data from column "Value1" accesable at the first position of the string array and "Value2" at the second position. 

## DBDestination

Like the `DBSource`, the `DBDestination` is the common component for sending data into a database. It is initalized with a table name or a `TableDefinition`.

The following example would transfer data from the destination to the source, using all the same connection manager (derived from `ControlFlow.CurrentDbConnection`):

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("SourceTable");
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("DestinationTable");
//Link everything together
source.LinkTo(dest);
//Start the data flow
source.Execute();
dest.Wait()
```

### Sql Server Connections

The `DBSource` and `DBDestination` can be used to connect via ADO.NET to a sql server. 
Use the `ConnectionString` object and a `SqlConnectionManger` to create a regular ADO.NET connection. 

```C#
DBSource source = DBSource (
    new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;")),
    "SourceTable"
);
```

### MySql Connections

The `DBSource` and `DBDestination` can be used to connect to a MySql database via the MySql ADO.NET provider.
Use the `MySqlConnectionString` object and a `MySqlConnectionManger`. 

```C#
DBSource source = DBSource (
    new MySqlConnectionManager(new MySqlConnectionString("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;")),
    "SourceTable"
);
```

### Postgres Connections

The `DBSource` and `DBDestination` can be used to connect to a Postgres database via the Postgres ADO.NET provider.
Use the `PostgresConnectionString` object and a `PostgresConnectionManger`. 

```C#
DBDestination dest = DBDestination (
    new PostgresConnectionManager(new PostgresConnectionString(""Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;")),
    "DestinationTable"
);
```

### SQLite Connections

The `DBSource` and `DBDestination` can be used to connect to a SQLite database via the SQLite ADO.NET provider.
Use the `SQLiteConnectionString` object and a `SQLiteConnectionManger`. 

```C#
DBSource source = DBSource (
    new SQLiteConnectionManager(new SQLiteConnectionString("Data Source=.\\db\\SQLiteControlFlow.db;Version=3;")),
    "SourceTable"
);
```

### SMO Connection Manager

The `SMOConnectionManager` uses Sql Server Managed Objects to connect to a Sql Server. It allow the use of the GO keyword within your SQL to separate batches. 
It can be used with a `ConnectionString`.

```C#
ControlFlow.CurrentDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;"));
SqlTask.ExecuteNonQuery("SQL with GO keyword", "CREATE SCHEMA TEST; GO; SELECT 1");
```

### Sql Server ODBC Connections

The `DBSource` and `DBDestination` also works with ODBC connection to Sql Server. . 
You will still use the underlying ADO.NET, but it allows you to connect to SQL Server via ODBC. 
  
```C#
DBSource source = DBSource (
    new SqlODBCConnectionManager(new ODBCConnectionString(""Driver={SQL Server};Server=.;Database=ETLBox_ControlFlow;Trusted_Connection=Yes")),
    "SourceTable"
);
```

*Warning*: ODBC does not support bulk inserts like in "native" connections.
The `DBDestination` will do a bulk insert by creating a sql insert statement that
has multiple values: INSERT INTO (..) VALUES (..),(..),(..)


### Access DB Connections

The ODBC connection to Microsoft Access databases have some more restrictions. ETLBox is based .NET Core and will only
support 64bit ODBC connections. You need also have Microsoft Access 64 bit installed. (The corresponding 64bit ODBC driver for Access can be download 
Microsoft: [Microsoft Access Database Engine 2010 Redistributable](https://www.microsoft.com/en-us/download/details.aspx?id=13255)
To create a connection to an Access Database, use the `AccessOdbcConnectionManager` and an `OdbcConnectionString`.

```C#
DBDestination dest = DBDestination (
    new AccessOdbcConnectionManager(new OdbcConnectionString("Driver={Microsoft Access Driver (*.mdb, *.accdb)}DBQ=C:\DB\Test.mdb")),
    "DestinationTable"
);
```

*Warning*: The `DBDestination` will do a bulk insert by creating a sql statement using a sql query that Access understands. The number of rows per batch is very limited - if it too high, you will the error message 'Query to complex'. Try to reduce the batch size to solve this.

*Note*: Please note that the AccessOdbcConnectionManager will create a "temporary" dummy table containing one record in your database when doing the bulk insert. After completion it will delete the table again. This was necessary to simulate a bulk insert with Access-like Sql. 
