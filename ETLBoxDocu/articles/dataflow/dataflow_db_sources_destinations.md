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

## Connection types

### Sql Server Connections

The `DBSource` and `DBDestination` can be used to connect via ADO.NET to a sql server. 
You can directly pass a connection string and the name of the source table. 
```C#
DBSource source = DBSource (
    new SqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;")
    , "SourceTable"
);
```

Additionally, for all connection managers you can pass a `ConnectionString` object which wraps the connection string. 

```C#
DBSource source = DBSource (
    new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;")),
    "SourceTable"
);
```


### MySql Connections

The `DBSource` and `DBDestination` can be used to connect to a MySql database via the MySql ADO.NET provider.
Either use the raw connection string or a `MySqlConnectionString` object and a `MySqlConnectionManger`. 

```C#
DBSource source = DBSource (
    new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;"),
    "SourceTable"
);
```

### Postgres Connections

The `DBSource` and `DBDestination` can be used to connect to a Postgres database via the Postgres ADO.NET provider.
Either use the raw connection string or a use the `PostgresConnectionString` object and a `PostgresConnectionManger`. 

```C#
DBDestination dest = DBDestination (
    new PostgresConnectionManager("Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;"),
    "DestinationTable"
);
```

### SQLite Connections

The `DBSource` and `DBDestination` can be used to connect to a SQLite database via the SQLite ADO.NET provider.
Either use the raw connection string or a use the `SQLiteConnectionString` object and a `SQLiteConnectionManger`. 

```C#
DBSource source = DBSource (
    new SQLiteConnectionManager("Data Source=.\\db\\SQLiteControlFlow.db;Version=3;"),
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

### ConnectionStrings

When you create a new connection manager, you have the choice to either pass the connection string directly or you
 create a `ConnectionString` object from the connection string before you pass it to the connection manager.
 The `ConnectionString` does exist for every database type (e.g. for MySql it is `MySqlConnectionString`). The ConnectionString
 wraps the raw database connection string into the appropriate `ConnectionStringBuilder` object and also offers some more
 functionalities, e.g. like getting a connection string for the database storing system information. 

## Connection Pooling

The implementation of all connection managers is based on Microsoft ADO.NET and makes use of the underlying 
connection pooling. [Please see here for more details of connection pooling.](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-connection-pooling)
This means that this actually can increase your performance, and in most scenarios you never have more 
connections open that you actually need for your application.

You don't need to explicitly open a connection. ETLBox will call the `Open()` method on a connection manager whenever
needed - where it relies on the underlying ADO.NET connection pooling that either creates a new connection 
or re-uses an existing one. Whenever the work of a component or task is done, the connection manager will return the connection back to 
the pool so that it can be reused by other components or tasks when needed.

Please note that the connection pooling only works for the same connection strings. For every connection string that differs there
is going to be a sepearate pool 

This behaviour - returning connections back to the pool when the work is done - does work very well in a scenario 
with concurrent tasks. There may be a use-case where you don't won't to query your database in parallel and you 
want to leave the connection open, avoiding the pooling. [For this scenario you can use the `LeaveOpen` property
on the connection managers.](https://github.com/roadrunnerlenny/etlbox/issues/39)



