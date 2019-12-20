# Working with data types

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

### ColumnMap attribute

The `ColumnMap` attribute is used whenever data is read from a database source or written into a database destination. When reading from 
a database source, it will tell the reader which database column name is mapped to the property. It will then write the data into the property via
the setter - method. When writing into a database destination, the attribute will tell in which database column the property data is written into. 
Here the getter - method is used to get data from the property.

For examle, if you have a property `Key`, and you add the `ColumnMap` Attribute to it: 

```C#
[ColumnMap("Id")]
public string Key { 
    get {
        return _key;
    set {
        _key = value.ToString();
    }

public int _key;
```

If you use this object within a `DBSource`, it will read the data from the database column "Id" and then call the `ToString` method on every record
before actually writing it into the property. 

Now you could add another property:

```C#
[ColumnMap("Hash")]
public string HashValue => HashHelper.Encrypt_Char40(this.Key);
```

When you write into a database table that has a column named "Hash", the column mappings will map the result of the HashValue -property
to this column. Every record is then stored in the table as an encrypted hash value of the property "Key". 

You can use this mapping behaviour for some basic data type transformations. If transformations become more complex, you should have a look at 
the [existing transformations](dataflow_transformations.md) to modify your data. 

## Non generic approach

Instead of parsing your source data into a object, you can just read all your data in to string array. This is equivalent to 
`DBSource<string[]>`.

E.g.:

```C#
DBSource source = new DBSource($@"select Value1, Value2 from dbo.Test");
RowTransformation row = new RowTransformation( 
    row => {
        string value1 = row[0];
        string value2 = row[1];
        }
);
```

would have all data from column "Value1" accesable at the first position of the string array and "Value2" at the second position. 
All your data will be automatically converted into a string data type. 

This approach is very useful when reading from a source where you get only string data, e.g. CSV or Json. 
You can use a `RowTransformation` if you want to convert your string array into an object.

```C#
RowTransformation<string[], MySimpleRow> = new RowTransformation<string[], MySimpleRow>( 
    row => {
        new MySimpleRow() {
            Col1 = row[0];
            Value2 = int.Parse(row[1]);
        }
);
```