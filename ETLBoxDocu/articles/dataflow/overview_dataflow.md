# A simple data flow

The main part of ETLBox is the Data Flow library. It basically is the ETL part, and holds all components
for extracting, transforming and loading data. 

All Data Flow taks reside in the 'ALE.ETLBox.DataFlow' namespace.

## What is a data flow?

You have some data somewhere - stored in some files, a table or somewhere else. 
Now you want to define a pipeline which takes this data, transforms it "on the fly" and writes it into a target 
(this could be again a database, a file or somewhere else). 
This is the pure essence of an ETL process (extracting, transformig, loading).
The building block to define such a data flow in ETLBox are source components for extracting, transformations
and destination components for loading.

## Components 

### Source components

All dataflow pipelines will need at least one or more sources. Sources are basically everything that can read data from someplace 
(e.g. CSV file or a database table) and then post this data into the pipeline. All sources should be able to read data asynchronously. 
That means, while the component reads data from the source, it simultanously sends the already processed data to components that are connected to source.
There are different build-in data sources, e.g.: `CSVSource`, `DBSource` or `ExelSource`. If you are in need of another source component, you can either extend the 
`CustomSource`. Or you [open an issue in github](https://github.com/roadrunnerlenny/etlbox/issues) describing your needs. 

Once a source starts reading data, it will start sending data to its connected components. These could be either a Transoformation or Destination.
Posting data is always done asynchrounously, even if you use the blocking Execute() method on the source.  

### Transformations

Transformations always have at least one input and one output. Inputs can be connected either to other transformations or 
sources, and the output can also connect to other transformationsor to destinations. 
The purpose of a transformation component is to take the data from its input(s) and post the transformed data to its outputs. 
This is done on a row-by-row basis for non-blocking transformation, or on a complete set for blocking transformations.
As soon as there is any data in the input, the transformation will start and post the result to the output. 

### Destination components 

Destination components will have normally only one input. They define a target for your data, e.g. a database table or CSV file. Currently, there is `DBDestination` 
and `CSVDestination` implemented. If you are in need of another destination component, you can either extend the `CustomDestination` or you [open an 
issue in github](https://github.com/roadrunnerlenny/etlbox/issues).

Every Destination comes with an input buffer. 

While a Destination for csv target will open a file stream where data is written into it as soon as arrives, 
a DB target will do this batch-by-batch - therefore, 
it will wait until the input buffer reaches the batch size (or the data is the last batch) and then insert 
it into the database using a bulk insert. 


## A simple dataflow

Let's look at a simple dataflow like this:

CSV File (Source) --> Row transformation --> DB destination.

### Creating the components 

First, we need a connection manager that stores the connections string and provides native ADO.NET to the database.
Always use the right connection manager for you database - e.g., the SqlConnectionManager will connect with 
a Sql Server (and expectes a sql server connection string). There are also other connection managers
(e.g. `SQLiteConnectionManager` for SQLite, `PostgresConnectionManager` for Postgres or `MySqlConnectionManager`
for MySql).

```C#
SqlConnectionManager connMan = new SqlConnectionManager("Data Source=.;Initial Catalog=demo;Integrated Security=false;User=sa;password=reallyStrongPwd123");
```

Now we need to create a source, in this example it could contain order data. This will look like this:

```C#
CSVSource source = new CSVSource("demodata.csv");
```

We now add a row transformation. The default output format of a `CSVSource` is an string array. In this example, 
we will convert the csv string array into an `Order` object and adding some logic while the transformation.

```C#
RowTransformation<string[], Order> rowTrans = new RowTransformation<string[], Order>(
    row => new Order()
    {
        Id = int.Parse(row[0]),
        Item = row[1],
        Quantity = int.Parse(row[2]) + int.Parse(row[3]),
        Price = double.Parse(row[4]) * 100
    });
```

*Please note that the `CSVSource` could be directly created as `CSVSource<OrderFile>`. Data type conversions 
(like `int.Parse()`) would then have beend handled internally by the CSVSource.*

Now we need to create a destination. Notice that the destination is typed with the `Order` object. We also
need to pass the connection manager to the DBDestination so that connection to our Sql Server can be used. 

```C#
DBDestination<Order> dest = new DBDestination<Order>(connMan, "OrderTable");
```

**If you don't want to pass the connection manager object over and over again to the your DataFlow or ControlFlow objects,
you can store a default connection in the static property `ControlFlow.CurrentDbConnection`.**

### Linking all together

Until now we have only created the components, but we didn't define the Data Flow pipe. Let's do this now:

```C#
sourceOrderData.LinkTo(rowTrans);
rowTrans.LinkTo(dest);
```

This will create a data  flow pipe CSVSource -> RowTransformation -> DBDestination

### Executing the dataflow

Now we will give the source the command to start reading data. 

```C#
  source.Execute();
``` 

This code will execute as an asynchronous task. If you want to wait for the Data Flow pipeline to finish, add this line to your code

```C#
dest.Wait();
```

When `dest.Wait()` returns, all data was read from the source and written into the database table. 

### View the full code

This demo code is available online  - [view the full code on github](https://github.com/roadrunnerlenny/etlboxdemo/tree/master/SimpeFlow).
