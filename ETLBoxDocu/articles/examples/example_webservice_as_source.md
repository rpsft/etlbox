# Example: Use Custom source to consume web service

## JsonSource

If you would like to use a WebAPI or REST service as data source, you should first check 
if the [current implementation of `JsonSource`](./example_web_services.md) is not already satisfying your needs. 

## Purpose

The example shows how the `CustomSource` can be used to query any kind of web service.

## Test web service 

In this example, I will use the JSONPlaceHolder project. It is a fake Online REST API for Testing and Prototyping.
See [jsonplaceholder.typicode.com](https://jsonplaceholder.typicode.com) for more details.


### Create necessary POCO

In order to store the data from the webservice, we need a POCO (Plain old Component object) to store an element in there. 
For this example, we create a representation for a Todo item.

```C#
public class Todo
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public string Title { get; set; }
    public bool Completed { get; set; }
}
```

## Connect with the webservice

Next, we need a class that connect to the webservice. This class has a method `ReadTodo()` which uses 
a simple HttpClient to connect to the service and to get the data for one Todo item. 
Every time it is called, the `TodoCounter` is increased by one. The method `EndOfData` returns true when the webservice 
was called more than 5 time.

```C#
public class WebserviceReader
{
    public string Json { get; set; }
    public int TodoCounter { get; set; } = 1;
    public Todo ReadTodo()
    {
        var todo = new Todo();
        using (var httpClient = new HttpClient())
        {
            var uri = new Uri("https://jsonplaceholder.typicode.com/todos/" + TodoCounter);
            TodoCounter++;
            var response = httpClient.GetStringAsync(uri).Result;
            Newtonsoft.Json.JsonConvert.PopulateObject(response, todo);
        }
        return todo;
    }

    public bool EndOfData()
    {
        return TodoCounter > 5;
    }
}
```

## Create a destination table

In this example, we want to store the data retrieved from the Webservice in a table in our database. 
For that, we need a table in our database. 

```C#

SqlTask.ExecuteNonQuery("Create test table",
    @"CREATE TABLE dbo.ws_dest 
    ( id INT NOT NULL, userId INT NOT NULL, title NVARCHAR(100) NOT NULL, completed BIT NOT NULL )"
);
```

## Linking it all together

Now we create a dataflow that calls the webservices and stores the result in the database.
We use the `CustomSource` for this - a custom source accepts a `Func` that is called to 
retrieve data and post it into the dataflow - it is called until the second `Func` is evaluated as true. 

```C#
WebserviceReader wsreader = new WebserviceReader();
CustomSource<Todo> source = new CustomSource<Todo>(wsreader.ReadTodo, wsreader.EndOfData);
DbDestination<Todo> dest = new DbDestination<Todo>("dbo.ws_dest");
source.LinkTo(dest);
```

Now we start the dataflow with:

```C#
source.Execute();
dest.Wait();
```





