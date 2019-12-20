# A basic example

This example will give you a brief overview of the basic concepts of ETLBox. 

## See the video

[There is also a video demonstrating this example](https://www.youtube.com/watch?v=CsWZuRpl6PA).

### Environment

For this demo you can use Visual Studio for Mac and Sql Server for linux running in a docker image. An User interface for managing sql server on Mac would be the Azure Data Studio.

First, we need to start a docker image running sql server on ubuntu. Run the following command line statement in the terminal to 
start up the container. 

```bash
docker run -d --name sql_server_demo -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=reallyStrongPwd123' -p  1433:1433 microsoft/mssql-server-linux
```

With the command ```docker ps``` we can see the container is up and running.


Now create a new dotnet core console application. Do this either with your GUI or execute the following command:

```dotnet new console```

Add the current version of ETLBox as a package to your project. 

```bash
dotnet add package ETLBox
```

Now you will be able to use the full set of tools coming with ETLBox


### Start coding
Now we are in the static main method. 

We need to store a connection string in the static Control Flow object.
```C#
 ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString
            ("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123"));
```

With CreateDatabaseTask we will create a new Database. 

```C#
CreateDatabaseTask.Create("demo");
```

Also we would like to change the connection to the database we just created and create a table in there using the CreateTableTask. 

```C#
ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString
("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123;Initial Catalog=demo"));

            CreateTableTask.Create("Table1", new List<TableColumn>()
            {
                new TableColumn("ID","INT",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Col1","NVARCHAR(100)",allowNulls:true),
                new TableColumn("Col2","SMALLINT",allowNulls:true)
            });
```

### Adding nlog.config
Before we test our demo project, we want to have some logging output displayed. ETLBox logging is build on nlog. On the etlbox website you will find examples how to configure logging with nlog. Add the following lines as nlog.config to your project root.
Make sure it is copied into the output directory.

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> 
  <rules>
    <logger name="*" minlevel="Debug" writeTo="console" />
  </rules>
  <targets>
    <target name="console" xsi:type="Console" />     
  </targets>
</nlog>
```

### Running the project

Now build and run the project.
A terminal window will pop up and display the logging output. As the logging level is set to debug, you will see all SQL code which is executed against the database.
Check if the database and the table was created.

### A simple etl pipeline

Next we want to create a simple etl pipeline. First we create a demo csv file named ```input.csv```. The input file contains header information and some value. Also we need to copy it into the output directory.

```csv
Col1,Col2
Value,1
Value2,2
Value3,3
```

Now we create a CSVSource pointing to the newly created input file. 

```C#
CSVSource source = new CSVSource("input.csv");
```

Before we continue, we will need an object that can hold our data. Let's call it MyData.

```C#
public class MyData
{
    public string Col1 { get; set; }
    public string Col2 { get; set; }
}
```

Now we add a row transformation. The row transformation will receive a string array from the source and transform it 
in our Mydata object.

```C#
RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>
(
    input => new MyData() 
    { Col1 = input[0], Col2 = input[1] }
);
```

Next we add a database destination pointing to our table.

```C#
DBDestination<MyData> dest = new DBDestination<MyData>("Table1");
```

Now we need to link the components of our dataflow.

```C#
source.LinkTo(row);
row.LinkTo(dest);
```

After linking the components, we want to have the source reading the input data.
The destination should wait until it received all data.

```C#
source.Execute();
dest.Wait();
```

Finlly, we check if the data was successfully loaded into the table and write it into the console output. We use the SQLTask for this.

```C#
SqlTask.ExecuteReader("Read all data from Table1",
    "select Col1, Col2 from Table1",
    col1 => Console.WriteLine(col1.ToString() + ","),
    col2 => Console.WriteLine(col2.ToString())
);
```

### Run again 
Let's run the project again and see the output.

You'll see that the data was successfully copied into the database table.

## Whole code

Here is the whole example code.

File Program.cs

```C#
using System;
using System.Collections.Generic;
using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString
            ("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123"));
            CreateDatabaseTask.Create("demo");
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString
("Data Source=.;Integrated Security=false;User=sa;password=reallyStrongPwd123;Initial Catalog=demo"));

            CreateTableTask.Create("Table1", new List<TableColumn>()
            {
                new TableColumn("ID","int",allowNulls:false, isPrimaryKey:true, isIdentity:true),
                new TableColumn("Col1","nvarchar(100)",allowNulls:true),
                new TableColumn("Col2","smallint",allowNulls:true)
            });

            CSVSource source = new CSVSource("input.csv");
            RowTransformation<string[], MyData> row = new RowTransformation<string[], MyData>(
            input => new MyData() { Col1 = input[0], Col2 = input[1] });
            DBDestination<MyData> dest = new DBDestination<MyData>("Table1");

            source.LinkTo(row);
            row.LinkTo(dest);
            source.Execute();
            dest.Wait();

            SqlTask.ExecuteReader("Read all data from table1",
            "select Col1, Col2 from Table1",
                col1 => Console.WriteLine(col1.ToString() + ","),
                col2 => Console.WriteLine(col2.ToString()));

        }

        public class MyData
        {
            public string Col1 { get; set; }
            public string Col2 { get; set; }
        }
    }
}
```

nlog.config

```xml
<?xml version="1.0" encoding="utf-8"?>
<nlog xmlns="http://www.nlog-project.org/schemas/NLog.xsd"
      xsi:schemaLocation="NLog NLog.xsd"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"> 
  <rules>
    <logger name="*" minlevel="Debug" writeTo="console" />
  </rules>
  <targets>
    <target name="console" xsi:type="Console" />     
  </targets>
</nlog>
```

input.csv
```csv
Col1,Col2
Value,1
Value2,2
Value3,3
```
