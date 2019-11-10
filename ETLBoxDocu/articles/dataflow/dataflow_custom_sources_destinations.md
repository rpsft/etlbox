# Custom Sources and Destinations

## Custom Source

A custom source can generate any type of of output you describe in a Function. There is a function that describe how the records
are generated and a function that returns true if you reached the end of your data. 

```C#
List<string> Data = new List<string>() { "Test1", "Test2", "Test3" };
int _readIndex = 0;
Func<MySimpleRow> ReadData = () =>
{
    var result = new MySimpleRow()
    {
        Col1 = _readIndex + 1,
        Col2 = Data[_readIndex]
    };
    _readIndex++;
    return result;
};

Func<bool> EndOfData = () => _readIndex >= Data.Count;

//Act
CustomSource<MySimpleRow> source = new CustomSource<MySimpleRow>(ReadData, EndOfData);
```

### Custom Destination

A custom destination calls the given action for every received record in the destination.

```C#
CustomDestination<MySimpleRow> dest = new CustomDestination<MySimpleRow>(
    row => {
        SqlTask.ExecuteNonQuery(Connection, "Insert row",
            $"INSERT INTO dbo.CustomDestination VALUES({row.Col1},'{row.Col2}')");
    }
);
```