# Overview Web service requests

## Test web service 

In this example, I will use the JSONPlaceHolder project. It is a fake Online REST API for Testing and Prototyping.
See [jsonplaceholder.typicode.com](https://jsonplaceholder.typicode.com) for more details.

## JsonSource

You can directly query web services or REST APIs using the `JsonSource`. There are some minor restrictions 
1) The http or https service has to send back a Json response
2) The json needs to be an array.

A good example for a valid json response that can be read by the `JsonSource` component is [this example web service](https://jsonplaceholder.typicode.com/todos/).

It will return a json in the following format:

```json
[
  {
    "userId": 1,
    "id": 1,
    "title": "delectus aut autem",
    "completed": false
  },
  ...
]
```

Please note that the root element is an array, and it does contain object definitions.

Here is an example how to read data from this web service.

First, you need to define your POCO:

```C#
public class Todo
{
    public int Id { get; set; }
    public int UserId { get; set; }
    public string Title { get; set; }
    public bool Completed { get; set; }
}
```

Now, you can simply read the data from the service by using the `JsonSource`:

```C#
JsonSource<Todo> source = new JsonSource<Todo>("https://jsonplaceholder.typicode.com/todos");
```

So a full example where you read data from a webservice e.g. into a memory data table would look like this:

```C#
JsonSource<Todo> source = new JsonSource<Todo>("https://jsonplaceholder.typicode.com/todos");
MemoryDestination<Todo> dest = new MemoryDestination<Todo>(200);

source.LinkTo(dest);
source.Execute();
dest.Wait();

//dest.Data will now contain all items from the web service
```

The property `Data` (which is a snychronized list) will now contain all items from the web service. 

### JsonPath

Sometimes, you don't want to create a full C# Poco (Plain old component object).
If you leave out some properties, you will see that the Json Deserializer just will ignore these properties. 

You can even use the JsonProperty attribute to add specific JsonPath expressions that the JsonDeserializer uses 
to map the right json records into the object.

E.g. if you want to have the Id-entry in the json mapped to property `Key` in your object definition, you would do it like this:

```C#
public class Todo
{
    [JsonProperty("Id")]
    public int Key { get; set; }
    public string Title { get; set; }
}
````

Also, this example does not have the properties `Completed` and `UserId` anymore - they will be ignored in the data flow

Deserialization is completely based on The [Newtonsoft.Json.JsonSerializer](https://www.newtonsoft.com/json/help/html/T_Newtonsoft_Json_JsonSerializer.htm).
There is also a property `JsonSerializer` that can be overwritten with your own JsonSerializer.

### String array

If you don't want to use objects, you can use the Non-generic version of `JsonSource`. Your code would look like this:

```C#
JsonSource source = new JsonSource("https://jsonplaceholder.typicode.com/todos");
MemoryDestination dest = new MemoryDestination();
```

Internally, a string array is used to store the data. 
Now you either have to override the `JsonSerializer` yourself in order to properly convert the json into a string[].
Or your Json has to be in following format:

```Json
[
    [
        "1",
        "Test1"
    ],
    ...
]
```
