# Overview Control flow tasks

To get aquainted with ETLBox, you should start with the Control Flow tasks. 

## Namespace

Control Flow task can be split in "General" task and "Logging" tasks. Control Flow Tasks reside in the `ALE.ETLBox.ControlFlow` namespace -
task for logging in the `ALE.ETLBox.Logging` namespace.

## Idea behind Control Flow Task

Control Flow Tasks are a comprehensive set of tasks to manage, alter or query a database. 
With one single line of code you will be able to create a table or fire a sql on your database. 
If you have ever did this before using ADO.NET, you probably found out that the is some boilerplate code you have to write over and over again. 
The idea behind the Control Flow Tasks is that you don't have to write the same code again and again, e.g. just for doing something trivial like opening up a connection 
and counting the rows in table. This should be doable with only one line of code.

### ADO.NET - the old way

For instance, the code for establishing a connection and doing a simple row count on a table with a classic ADO.NET connection would look like this:

```C#
string connectionString = "Data Source=.; Database=Sample; Integrated Security=SSPI";
using (SqlConnection con = new SqlConnection(connectionString))
{
   SqlCommand cmd = new SqlCommand("select count(*) from dbo.tbl", con);
   con.Open();
   int numrows = (int)cmd.ExecuteScalar();   
}
```

### RowCount with Control Flow Tasks

Now let's have a look how to make a row count with the Control Flow Tasks library. 

First, we need to setup a connection
With Control Flow Task you only have to set up your database connection string once, like this:

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.; Database=Sample; Integrated Security=SSPI""));
```

*The connection will be stored in a static property and used by all subsequent tasks if no other connection is passed when a tasks is executed.*

Now you can use a `RowCountTask` to query the number of rows within a table with only one line.

```C#
int count = RowCountTask.Count("dbo.tbl");
```

Internally, an ADO.NET connection is opened up (the default ADO.NET connection pooling is used) and a `select count(*) from dbo.tbl` is executed on the database. 
The result is return from the RowCountTask. 

### Using the instances

[!NOTE]
<For every Control Flow Tasks, there are static accessors to simplify the use of the tasks. In order to have access to all functionalities of a task, sometime you 
have to create a new instance of the task object.>

If you want to do a rowcount with an instance instead of the static accessor, it would look like this:
```C#
RowCountTask task = new RowCountTask("dbo.tbl");
int count = task.Count().Rows;
```

So whenever you don't find a static accessor satisfing your needs, try to create an instance and check the properties and methods of the object.


## Why not Entitiy Framework

ETLBox was designed to be used as an ETL object library. Therefore, the user normally deals with big data, some kind of datawarehouse structures and is used to
have full control over the database. With the underlying power of ADO.NET - which is used by ETLBox - you have full access to the database and basically can do anything 
you are used to do with sql on the server. As EF (Entity Framework) is a high sophisticated ORM tool, it comes with the downside that you can only do things on a database that
EF allows you to do. But as EF does not incorporate all the possibilites that you can do with SQL and ADO.NET on a Sql Server, Entitity Framework normally isn't a 
good choice for creating ETL jobs. This is also true for other ORM tools.


## Configure a tasks

But there is more. Let's assume you want to count the rows on a pretty big table, a "normal" row count perhaps would take some time. So RowCount has a property called
`QuickQueryMode`. If set to true, a sql statement that queries the partition tables is then executed. 

```C#
RowCountTask task = new RowCountTask("dbo.tbl") 
	{ QuickQueryMode = true };
int count = task.Count().Rows;
```

This would give you the same result, but insteal of doing a `select count(*) from dbo.tbl` the following sql is fired on the database
```sql
select cast(sum([rows]) as int) from sys.partitions where [object_id] = object_id(N'dbo.tbl') and index_id in (0,1)
```

### Further reading

Now you familiar with the basic concepts of Control Flow Task. Either continue with the [Control Flow Example](examples/example_controlflow.md) 
or have a look at the [API reference](../api/index.md).