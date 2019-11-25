# Generic and non-generic approach

## Generic approach

Almost all components in ETLBox can be defined with a POCO that matches the data types of the data. 
By default, the mapping of column names to properties is resolved by the property name itself. E.g. a column named Value1 
would stored in the property with the same name. If you use the `ColumnMap` attribute, you can add what column name will be mapped 
to the property. If there is no match, the column will be ignored.

Usage example:

```C#
public class MySimpleRow {
    [ColumnMap("Value1")]
    public string Col1 { get; set; }
    public int Value2 { get; set; }
}

DBSource<MySimpleRow> source = new DBSource<MySimpleRow>(
    $@"select Value1, Value2 from dbo.Test"
);
```

## Non generic approach

Instead of parsing your source data into a object, you can just read all your data in to string array. This is equivalent to 
`DBSource<string[]>`.

E.g.:

```
DBSource source = new DBSource($@"select Value1, Value2 from dbo.Test");
RowTransformation row = new RowTransformation( 
    row => {
        string value1 = row[0];
        string value2 = row[1];
        }
);
```

would have all data from column "Value1" accesable at the first position of the string array and "Value2" at the second position. 

