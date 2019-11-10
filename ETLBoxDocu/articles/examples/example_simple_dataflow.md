# A simple dataflow

Let's look at a simple dataflow like this:

CSV File (Source) --> Row transformation --> DB destination.

## Setting up the connection

As the Data Flow Tasks are based on the same foundament like the Control Flow Tasks, you first should set up a connection like you do for
a Control Flow Task.

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString("Data Source=.;Integrated Security=SSPI;"));
```

## Creating the source 

Now we need to create a source, in this example it could contain order data. This will look like this:

```C#
CSVSource sourceOrderData = new CSVSource("demodata.csv");
```

## Creating the row transformation

We now add a row transformation. The default output format of a `CSVSource` is an string array. In this example, we will convert the csv string array into an `Order` object.

```C#
RowTransformation<string[], Order> rowTrans = new RowTransformation<string[], Order>(
  row => new Order(row)
);    
```

## Creating the destination 

Now we need to create a destination. Notice that the destination is typed with the `Order` object.

```C#
DBDestination<Order> dest = new DBDestination<Order>("dbo.OrderTable");
```

## Linking all together

Until now we have only created the components, but we didn't define the Data Flow pipe. Let's do this now:

```C#
sourceOrderData.LinkTo(rowTrans);
rowTrans.LinkTo(dest);
```

This will create a data  flow pipe CSVSource -> RowTransformation -> DBDestination

## Starting the dataflow

Now we will give the source the command to start reading data. 

```C#
  source.Execute();
``` 

This code will execute as an asynchronous task. If you want to wait for the Data Flow pipeline to finish, add this line to your code

```C#
dest.Wait();
```

When `dest.Wait()` returns, all data was read from the source and written into the database table. 
