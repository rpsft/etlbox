# <span><img src="https://github.com/roadrunnerlenny/etlbox/raw/master/docs/images/logo_orig_32x32.png" alt="ETLBox logo" height="32" /> ETLBox</span>

A lightweight ETL (extract, transform, load) library and data integration toolbox for .NET. Source and destination components let you read and write data from the most common databases and file types. Transformations allow you to you harmonize, filter, aggregate, validate and clean your data.

Create your own tailor-made data flow with your .NET language of choice. ETLBox is written in C# and offers full support for .NET Core. 

<div class="hideOnWebsite">

## ETLBox.net

[For full documentation visit the project homepage: etlbox.net](https://etlbox.net). You will find a whole set of introductional articles, lots of examples and a complete API documentation.

</div>

## Why ETLBox

ETLBox is a comprehensive C# class library that is able to manage your whole [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) or [ELT](https://en.wikipedia.org/wiki/Extract,_load,_transform).  You can use it to create your own dataflow pipelines programmatically in .NET, e.g. with C#. Besides a big set of dataflow components it comes which some control flow task that let you easily manage your database or simple execute Sql code without any boilerplate code. It also offers extended logging capabilites based on NLog to monitor and anlayze your ETL job runs.

ETLBox is a fully functional alternative to other ETL tools like Sql Server Integrations Services (SSIS). Creating your ETL processes programatically has some advantages: 

**Build ETL in .NET**: Code your ETL with your favorite .NET language fitting your team’s skills and that is coming with a mature toolset.

**Runs everywhere**: ETLBox runs on Linux, macOS, and Windows. It is written in the current .NET Standard and successfully tested with the latest versions of .NET Core & .NET.

**Run locally**: Develop and test your ETL code locally on your desktop using your existing development & debugging tools.

**Process In-Memory**: ETLBox comes with dataflow components that allow in-memory processing which is much faster than storing data on disk and processing later.

**Manage Change**: Track you changes with git (or other source controls), code review your etl logic, and use your existing CI/CD processes.

**Data integration**: While supporting different databases, flat files and web services, you can use ETLBox as a foundation for your custom made Data Integregation platform.

**Made for big data**: ETLBox relies on [Microsoft's TPL.Dataflow library](https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/dataflow-task-parallel-library) and was designed to work with big amounts of data.

### Data integration

The heart of an ETL framework is it's ability to integrate with other systems. 
The following table shows which types of sources and destination are supported out-of-the box with the current version of ETLBox.
**You can *always* integrate any other system not listed here by using a `CustomSource` or `CustomDestination` - though you have to write the integration code yourself.**

Source or Destination type|System name or file type|Limitations|
--------------------------|------------------------|------------
Databases|Sql Server, Postgres, SQLite, MySql|Full support
Flat files|Csv, Json, Xml|Full support
Office|Microsoft Access, Excel|Full support for Access, Excel only as source
Cube|Sql Server Analysis Service|Only XMLA statements
Any other|Supported with custom written sources & destinations|No limitations 

### Transformations

ETLBox has 3 type of transformations: Non-blocking, partially blocking and blocking transformations. Non-blocking transformations will
only store the row that is currently processed in memory (plus some more in the buffer to optimize throughput and performance). Partially blocking transformations will load some data in the memory before they process data row-by-row. Blocking transformations will wait until all data has arrived at the component before it starts processing all records subsequently. 

The following table is an overview of the most common transformations in ETLBox:  

Non-blocking|Partially blocking|Blocking|
------------|------------------|---------
RowTransformation|LookupTransformation|BlockTransformation
Aggregation|CrossJoin|Sort
MergeJoin||
Multicast||

## Overview

ETLBox is split into two main components: Data Flow and Control Flow Tasks. The Data Flow part offers the core ETL components. The tasks in the Control Flow allow you to manage your databases with a simple syntax. Both components come with customizable logging functionalities.







### Data Flow overview

Data flow tasks gives you the ability to create your own pipeline where you can send your data through. Data flows consists of one or more source element (like CSV files or data derived from a table), some transformations and optionally one or more target. To create your own data flow , you need to accomplish three steps:

- First you define your dataflow components
- You link these components together (each source has an output, each transformation at least one input and one output and each destination has an input)
- After the linking you just tell your source to start reading the data.

The source will start reading and post its data into the components connected to its output. As soon as a connected component retrieves any data in its input, the component will start with processing the data and then send it further down the line to its connected components. The dataflow will finish when all data from the source(s) are read and received from the destinations.

Of course, all data is processed asynchronously by the components. Each compoment has its own set of buffers, so while the source is still reading data, the transformations  can already process it and the destinations can start writing the processed information into their target.

There are a lot of pre-defined components in ETLBox available. The `DBSource` can connect to a SqlServer, SQLite, MySql or Postgres table. There are transformation to modify either each record one-by-one or in whole. You can split or join different sources, and using the `CustomSource` and `CustomDestination` you can even connect to any target or source you want.

#### Data Flow quick example

It's easy to create your own data flow pipeline. This example data flow will transfer data from a MySql database into a Sql Server database and transform the records on the fly.

Just create a source, some transformation and a destination:

```C#
var sourceCon = new MySqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;");
var destCon = new SqlConnectionManager("Data Source=.;Integrated Security=SSPI;Initial Catalog=ETLBox;");

DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(sourceCon, "SourceTable");
RowTransformation<MySimpleRow, MySimpleRow> trans = new RowTransformation<MySimpleRow, MySimpleRow>(
    myRow => {  
        myRow.Value += 1;
        return myRow;
    });
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(destCon, "DestinationTable");
```

Now link these elements together.

```C#
source.LinkTo(trans);
trans.LinkTo(dest);
```

Finally, start the dataflow at the source and wait for your destination to rececive all data (and the completion message from the source).

```C#
source.Execute();
dest.Wait();
```

#### Data Flow components

There are a lot of data flow components that you can choose from to create a data flow that suits all your needs. 

You can choose between different sources and destination components. `DBSource` and `DBDestination` will connect to the most used databases (e.g. Sql Server, Postgres, MySql, SQLite). `CSVSource`, `CSVDestination` give you support for flat files - [based on CSVHelper](https://joshclose.github.io/CsvHelper/). `ExcelSource` allows you to read data from an excel sheet. `JsonSource` and `JsonDestination` let you read and write json from files or web service request. `MemorySource`, `MemoryDestinatiation` as well as `CustomSource` and `CustomDestination` will give you a lot flexibility to read or write  data directly from memory or to create your own custom made source or destination component.

Once your data goes throug your data flow, it will be processed in-memory (either row-by-row or in batches), and there a lot of transformations you can choose from to transform your data. On a row-by-row basis, you can use the `RowTransformation` to modify each record with your custom written code. Data can be split within the flow by using a `Multicast` or joined with a `Mergejoin`. There is `Lookup` component to enrich your data with your existing master data.  You can sort your data using the `Sort`. 
For operations that need to access your incoming data in whole there is the `BlockTransformation`.

#### Designed for big data

ETLBox was designed for performance and is able to deal with big amounts of data. All destinations do support Bulk or Batch operations. By default, every component comes with an input and/or output buffer. You can design your data flow that only batches or your data is stored in memory, which are kept in different buffers for every component to increase throughput. All operations can be execute asynchrounously, so that your processing will run only within separate threads.

### Control Flow - overview

Control Flow Tasks gives you control over your database: They allow you to create or delete databases, tables, procedures, schemas or other objects in your database. With these tasks you also can truncate your tables, count rows or execute *any* sql you like. Anything you can script in sql can be done here - but with only one line of easy-to-read C# code. This improves the readability of your code a lot, and gives you more time to focus on your business logic. But Control Flow tasks are not restricted to databases only: e.g. you can run an XMLA on your Sql Server Analysis Service.

#### Control Flow quick example

It is now very easy to execute some Sql on the Database, without writing the whole "boilerplate" code by ADO.NET.

```C#
var conn = new SqlConnectionManager("Server=10.37.128.2;Database=ETLBox_ControlFlow;Uid=etlbox;Pwd=etlboxpassword;");
//Execute some Sql
SqlTask.ExecuteNonQuery(conn, "Do some sql",$@"EXEC myProc");
//Count rows
int count = RowCountTask.Count(conn, "demo.table1").Value;
//Create a table
CreateTableTask.Create(conn, "Table1", new List<TableColumn>() {
    new TableColumn(name:"key",dataType:"INT",allowNulls:false,isPrimaryKey:true, isIdentity:true),
    new TableColumn(name:"value", dataType:"NVARCHAR(100)",allowNulls:true)
});
```

### Logging

By default, ETLBox uses and extends [NLog](https://nlog-project.org). ETLBox already comes with NLog as dependency - so you don't need to include additional packages from nuget. In order to have the logging activating, you just have to set up a nlog configuration called `nlog.config`, and create a target and a logger rule. After adding this, you will already get logging output for all tasks and components in ETLBox. [Read more about logging here](https://etlbox.net/articles/overview_logging.html).

## Getting ETLBox

You can use ETLBox within any .NET or .NET core project that supports .NET Standard 2.0. (Basically all latest versions of .NET)

### Variant 1: Nuget

[ETLBox is available on nuget](https://www.nuget.org/packages/ETLBox). Just add the package to your project via your nuget package manager.

### Variant 2: Download the sources

Clone the repository:

```bash
git clone https://github.com/roadrunnerlenny/etlbox.git
```

Then, open the downloaded solution file ETLBox.sln with Visual Studio 2019 or higher.
Now you can build the solution, and use it as a reference in other projects.

## Going further

ETLBox is open source. Feel free to make changes or to fix bugs. Every particiation in this open source project is appreciated.

To dig deeper into it, have a look at the test projects. There is a test for (almost) everything that you can do with ETLBox.

<span class="hideOnWebsite">

[See the ETLBox Project website](https://etlbox.net) for [introductional articles](https://etlbox.net/articles/getting_started.html) and a [complete API documentation](https://etlbox.net/api/index.html). Enjoy!

</span>
