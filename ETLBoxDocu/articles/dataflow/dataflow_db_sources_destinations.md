# Database integration

There a numerous database sources and destinations that come with ETLBox. In short, you can extract data 
from and load data into the following databases: Sql Server, MySql, Postgres, SQLite and Microsoft Access.

## DBSource

The DBSource is the most common data source for a data flow. It basically connects to a database via ADO.NET and executes a SELECT-statement to start reading the data. 
While ADO.NET is reading from the source, data is simultaneously posted into the dataflow pipe.
To initialize a DBSource, you can either hand over a `TableDefinition`, a SQL-statement or a table name. 
The DBSource also accepts a connection manager. If no connection manager is specified, the "default" connection manager is used 
(which is stored in `ControlFlow.DefaultDbConnection`).

The following code would read all data from the table `SourceTable` and use the default connection manager:

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("SourceTable");
```

For the `DBSource`, you can also specify some Sql code to retrieve your data:

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>() {
    Sql = "SELECT * FROM SourceTable"
};
```

## DBDestination

Like the `DBSource`, the `DBDestination` is the common component for sending data into a database. It is initialized with a table name or a `TableDefinition`.

The following example would transfer data from the destination to the source, using all the same connection manager (derived from `ControlFlow.DefaultDbConnection`):

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>("SourceTable");
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>("DestinationTable");
//Link everything together
source.LinkTo(dest);
//Start the data flow
source.Execute();
dest.Wait()
```

## Connection manager

### Connection string

To connect to your database of choice, you will need a string that contains all information needed to connect
to your database (e.g., the network address of the database, user name and password). The specific connection string syntax 
for each provider is defined by the ADO.NET framework. If you need assistance
to create such a connection string, <a href="https://www.connectionstrings.com" target="_blank">have a look at this website that 
provide example string for almost every database</a>.

### Database Connections

The `DBSource` and `DBDestination` can be used to connect via ADO.NET to a database server. 
To do so, it will need the correct connection manager and either a raw connection string or a `ConnectionString` object. 
The easiest way would be to directly pass a raw connection string and connection manager.  

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

#### MySql Connections

The `DBSource` and `DBDestination` can be used to connect to a MySql database via the MySql ADO.NET provider.
Either use the raw connection string or a `MySqlConnectionString` object and a `MySqlConnectionManger`. 

```C#
DBSource source = DBSource (
    new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;"),
    "SourceTable"
);
```

#### Postgres Connections

The `DBSource` and `DBDestination` can be used to connect to a Postgres database via the Postgres ADO.NET provider.
Either use the raw connection string or a use the `PostgresConnectionString` object and a `PostgresConnectionManger`. 

```C#
DBDestination dest = DBDestination (
    new PostgresConnectionManager("Server=10.37.128.2;Database=ETLBox_DataFlow;User Id=postgres;Password=etlboxpassword;"),
    "DestinationTable"
);
```

#### SQLite Connections

The `DBSource` and `DBDestination` can be used to connect to a SQLite database via the SQLite ADO.NET provider.
Either use the raw connection string or a use the `SQLiteConnectionString` object and a `SQLiteConnectionManger`. 

```C#
DBSource source = DBSource (
    new SQLiteConnectionManager("Data Source=.\\db\\SQLiteControlFlow.db;Version=3;"),
    "SourceTable"
);
```

#### SMO Connection Manager

The `SMOConnectionManager` uses Sql Server Managed Objects to connect to a Sql Server. It allow the use of the GO keyword within your SQL to separate batches. 
It can be used with a `ConnectionString`.

```C#
ControlFlow.DefaultDbConnection = new SMOConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;"));
SqlTask.ExecuteNonQuery("SQL with GO keyword", "CREATE SCHEMA TEST; GO; SELECT 1");
```

#### Sql Server ODBC Connections

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


#### Access DB Connections

The ODBC connection to Microsoft Access databases have more restrictions. ETLBox is based .NET Core and will run in your application as dependency.
It now depends if you compile your application with 32bit or 64bit (some version of .NET Core only support 64bit). You will need
the right Microsoft Access driver installed - either 32bit or 64bit. You can only install the 32bit driver
if you have a 32bit Access installed, and vice versa. Also, make sure to set up the correct ODBC connection (again, there is 
64bit ODBC connection manager tool in windows and a 32bit). 

The corresponding 64bit ODBC driver for Access can be download 
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

### Connection String wrapper

When you create a new connection manager, you have the choice to either pass the connection string directly or you
 create a `ConnectionString` object from the connection string before you pass it to the connection manager.
 The `ConnectionString` does exist for every database type (e.g. for MySql it is `MySqlConnectionString`). The ConnectionString
 wraps the raw database connection string into the appropriate `ConnectionStringBuilder` object and also offers some more
 functionalities, e.g. like getting a connection string for the database storing system information. 

### Default ConnectionManager

Every component or task related to a database operation needs to have a connection managers set in order
to connect to the right database. Sometimes it can be cumbersome to pass the same connection manager over and over
again. To avoid this, there is a static `ControlFlow` class that contains the property `DefaultDbConnection`.
If you define a connection manager here, this will always be used as a fallback value if no other connection manager property was defined.

```
ControlFlow.DefaultDbConnection = new SqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;");
//Now you can just create a DBSource like this
var source = new DBSource("SourceTable");
```

### Connection Pooling

The implementation of all connection managers is based on Microsoft ADO.NET and makes use of the underlying 
connection pooling. [Please see here for more details of connection pooling.](https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-connection-pooling)
This means that this actually can increase your performance, and in most scenarios you never have more 
connections open that you actually need for your application.

You don't need to explicitly open a connection. ETLBox will call the `Open()` method on a connection manager whenever
needed - where it relies on the underlying ADO.NET connection pooling that either creates a new connection 
or re-uses an existing one. Whenever the work of a component or task is done, the connection manager will return the connection back to 
the pool so that it can be reused by other components or tasks when needed.

Please note that the connection pooling only works for the same connection strings. For every connection string that differs there
is going to be a separate pool 

This behavior - returning connections back to the pool when the work is done - does work very well in a scenario 
with concurrent tasks. There may be a use-case where you don't won't to query your database in parallel and you 
want to leave the connection open, avoiding the pooling. [For this scenario you can use the `LeaveOpen` property
on the connection managers.](https://github.com/roadrunnerlenny/etlbox/issues/39)


## Table Definitions

If you pass a table name to a `DBsource` or `DBDestination` (or a Sql statement to a `DBSource`), the meta data
of the table is automatically derived from that table or sql statement. If you don't want ETLBox to read this information
from the table, or if you want to provide your own meta information, you can pass a `TableDefinition` instead.

This could look like this:

```
var TableDefinition = new TableDefinition("tableName"
    , new List<TableColumn>() {
    new TableColumn("Id", "BIGINT", allowNulls:false,  isPrimaryKey: true, isIdentity:true)),
    new TableColumn("OtherCol", "NVARCHAR(100)", allowNulls: true)
});

var DBSource<type> = new DBSource<type>() {  
  SourceTableDefinition = TableDefinition
}
```

ETLBox will use this meta data instead to connect with the table. 