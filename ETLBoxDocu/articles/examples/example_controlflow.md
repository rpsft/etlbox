# Example Control Flow

Control Flow tasks are a great way to alter a database and run some sql against it. 
This example will give you a quick example how to create, alter and query a database.

## Prerequisites

If you want to run this example on your system, please use nuget to add the package as reference.
How to install the nuget package is written on the [Nuget package description page for ETLBox](https://www.nuget.org/packages/ETLBox/)
Alternatively, you can use the [Github repo] to clone and build the ETLBox project yourself. Visual Studio 2017 or higher is recommended.

## Namespace

The namespace for all objects in ETLBox is ```ALE.ETLBox```

## Connection String to database

First of all, you will need a connection string tied to your code. To avoid handing over this string again and again, you can preserve it in a static object - the ControlFlow object. It's purpose is to store parameters that are used in every ControlFlow Task. 

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
```

## Creating a database 

You can now simply create a database with

```C#
CreateDatabaseTask.Create("DemoDB");
```

If you want to drop it again, simple add ```C# DropDatabaseTask.Delete("DemoDB");``` before you create. 
Drop database won't give you an exception, even if the database does not exists. 

## Changing the connection

Now you need to change the connection string to the database you just created:

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;Initial Catalog=DemoDB;"));
```

## Altering the database 

Let's now create a new schema and a table on the database.

```C#
CreateSchemaTask.Create("demo");
CreateTableTask.Create("demo.table1", new List<TableColumn>() {
   new TableColumn(name:"key",dataType:"int",allowNulls:false,isPrimaryKey:true, isIdentity:true),
   new TableColumn(name:"value", dataType:"nvarchar(100)",allowNulls:true)
});
```
There is a ```TableColumn``` object that helps you to define columns in the table. 

## Running sql on the database

The most powerful and comman task that you want to run against a database is sql. It will use the underlying ADO.NET connection manager, which allows you to do almost everything on the database, without the "overhead" and boilerplate code that ADO.NET brings with it. 

```C#
SqlTask.ExecuteNonQuery("Insert data",
   $@"insert into demo.table1 (value) select * from (values ('Ein Text'), ('Noch mehr Text')) as data(v)");
```

## Using further tasks 

There are quite a lot of different control flow tasks that ETLBox comes with. 

A simple example is the RowCountTask, which unsuprisingly count the rows in a table.

```C#
int count = RowCountTask.Count("demo.table1");
Debug.WriteLine($"Found {count} entries in demo table!");
```

## Truncating a table

Truncating a table is as simple as

```C#
TruncateTableTask.Truncate("demo.table1");
```

## Creating a stored procedure

To create or alter a stored procedure in the database, you execute this:

```C#
CRUDProcedureTask.CreateOrAlter("demo.proc1", "select 1 as test");
```