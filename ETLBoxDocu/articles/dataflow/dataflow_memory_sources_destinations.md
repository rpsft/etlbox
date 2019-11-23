# Overview Web service requests

## MemorySource

A Memory source is a simple source comnponents that accepts a list. Use this component
within your dataflow if you already have you collection or records available in memory.
When you execute the flow, the memory destination will iterate throught the list and 
asynchronusly post record by record into the flow.

Example code:

```C#
 TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture("MemoryDestination");
MemorySource<MySimpleRow> source = new MemorySource<MySimpleRow>();
DBDestination<MySimpleRow> dest = new DBDestination<MySimpleRow>(SqlConnection, "MemoryDestination");
       
source.Data = new List<MySimpleRow>()
{
    new MySimpleRow() { Col1 = 1, Col2 = "Test1" },
    new MySimpleRow() { Col1 = 2, Col2 = "Test2" },
    new MySimpleRow() { Col1 = 3, Col2 = "Test3" }
};
source.LinkTo(dest);
source.Execute();
dest.Wait();
```

## MemoryDestination

A memory destination is a component that store the incoming data within a [BlockingCollection](https://docs.microsoft.com/de-de/dotnet/api/system.collections.concurrent.blockingcollection-1?view=netframework-4.8).
It stores the data within the `Data` property.
Data can be read from this collection as soon as it arrives. 

Example:

```C#
DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(SqlConnection, "MemoryDestinationSource");
MemoryDestination<MySimpleRow> dest = new MemoryDestination<MySimpleRow>();

source.LinkTo(dest);
Task st = source.ExecuteAsync();
Task dt = dest.Completion();

// data is acessable in dest.Data
```

When starting the data flow asynchronous, you should wait until the tasks complete. The source task will complete when 
all data was posted into the data flow, and the destination task completes when all data has arrived in the destination. 
