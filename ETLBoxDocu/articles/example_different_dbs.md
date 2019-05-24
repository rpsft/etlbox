# Example: Data flow across different Databases

## Purpose
Sometimes you are in need to transfer data across databases or across server. ETLBox was desinged with this
scenario always in mind and offers the ability to define the connection manager for eaach task. 
This example will guide you through a basic scenario.

## Implement the dataflow

### Understand the default ConnectionManager

The `ControlFlow` contains a default ConnectionManager property that is always used as a fallback value if 
no other connection manager property was defined.

### Creating the tables

First, we define two tables in different databases, and create log tables in a log database.
To do this, we use the CurrentDBConnection property in the
ControlFlow object, and change it value always to the right connection. But only for the source table and log tables.
For the destination, we use the property ConnectionManager of the CreateTableTask.

Please note that almost every task or component (ControlFlow and Dataflow!) has a ConnectionManager property that
can be set. This is only available if you use instances of the classes. Only a few classes have a static constructor method
that also accepts a ConnectionManager.

```C#
//CurrentDbConnection is always use if ConnectionManager is not specified otherwise!
ControlFlow.CurrentDbConnection = new SqlConnectionManager("Connection String Source");

SqlTask.ExecuteNonQuery("Create source table", @"CREATE TABLE test.Source
                (Col1 nvarchar(100) null, Col2 int null)");
SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test1',1)");
SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test2',2)");
SqlTask.ExecuteNonQuery("Insert demo data", "insert into test.Source values('Test3',3)");

new CreateTableTask("test.Destination", new List<ITableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            })
{
    ConnectionManager = new SqlConnectionManager("Connection String Destination")
}.Execute();

ControlFlow.CurrentDbConnection = new SqlConnectionManager("Connection String Logging DB");
CreateLogTablesTask.CreateLog();
```

### Defining the dataflow

Next, we define a `DBSource` and `DBDestination` with a `RowTransformation` in between.
Please note that you can pass the connection manager as part of the static constructor. 
In the RowTransformation, we use the LogTask. As no connection manager is specified here (which you could),
the fallback connection value from the `ControlFlow.CurrentDBConnection` is used. The last time we set this it pointed
to the logging database connection, so we are fine here.

```C#
DBSource source = new DBSource(new SqlConnectionManager(new ConnectionString("Connection string Source))
                                , "test.Source");

RowTransformation trans = new RowTransformation(row =>
{
    LogTask.Info($"Test message: {row[0]}, {row[1]}"); //Log DB is used as this is the ControlFlow.CurrentDBConnection!
    return row;
});

DBDestination destination = new DBDestination("test.Destination") {
    ConnectionManager = new SqlConnectionManager(new ConnectionString("Connection String Destination"))
};
```

### Linking everything together

Finally, we link our components and start the dataflow.

```C#
source.LinkTo(trans);
trans.LinkTo(destination);
source.Execute();
destination.Wait();
```