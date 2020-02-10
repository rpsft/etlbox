# Example: Writing into a json file

## JsonDestination

If you would like to write data into a json file, you should first see 
if the [current implementation of `JsonDestination`](../dataflow/dataflow_file_sources_destinations.md) is not already satisfying your needs. 

## Purpose

Sometimes you want to extract data and store it into a json file or use the json to send it to an Rest API. 
This example shows you how to use the `CustomDestination` to create a json. 

## Create necessary POCO

In this example we will receive some demo data from a database table. The table has two columns - `Col1` and `Col2`. 
In order to store the data from table, we first create a POCO (Plain old Component object) to use in the dataflow. 

```C#
public class MySimpleRow {
    public string Col1 { get; set; }
    public int Col2 { get; set; }
}
```

## Implement the dataflow

First, we will read the data from the source. We create a table use a `TableDefinition` object and pass this
to the `DbSource`.

```C#
TableDefinition sourceTableDefinition = new TableDefinition("test.Source"
            , new List<TableColumn>() {
                new TableColumn("Col1", "nvarchar(100)", allowNulls: false),
                new TableColumn("Col2", "int", allowNulls: true)
            });
sourceTableDefinition.CreateTable();
SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test1',1)");
SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test2',2)");
SqlTask.ExecuteNonQuery("Insert demo data", $"insert into {tableName} values('Test3',3)");

DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(sourceTableDefinition);
```

Next, we need to have the Destination defined. We create a `CustomDestination` that basically reads all the rows
into a List object. Be careful when you do this with huge amount of data, as all data will be stored in memory. 

```C#
List<MySimpleRow> rows = new List<MySimpleRow>();
CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
    row => {
        rows.Add(row);
    }
);
```

Then, we connect the source with destination and start the dataflow.

```C#
source.LinkTo(dest);
source.Execute();
dest.Wait();
```

Now that all data is in the List object, we can serialize it using `JsonConvert`. 

```C#
string json = JsonConvert.SerializeObject(rows, Formatting.Indented);
```

This json string can now be saved into a file or send to a web service. 

## Big data

If you need to write a big amount of data into a json, I recommend that you write the data into the file while reading it.
Instead of you a List object, you could you some kind of File stream in which you write while receive the row within the 
`CustomDestination`. But in this case you have to make sure that your json is valid and you have to take care of the
formatting etc. yourself.