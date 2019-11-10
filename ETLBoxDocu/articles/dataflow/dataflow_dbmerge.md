# Merging and syncing tables

ETLbox can be used to integrate data from different source and write them into different destinations. 
Most of the time you will use tables in a database as target. 
A very common use case here is to keep a source and destination tables "in sync". 
The following article describes how you can use the data from your data flow to insert 
new data in a destination table, update existing or delete removed records.

Please note that for the sake of simplification we use a Database table as source which we want to 
sync with a destination Database table. But of course the source doesn’t necessarily needs to be a tables - 
every data that comes from a source or transformation in your data flow can be used as source for syncing.

## Full or Delta

First, we need to differentiate 2 scenarios.

Scenario 1: The source table does not have any information about its changes. 
So no delta information is provided by the source.

Scenario 2: The source table has information about its changes. 
This can be e.g. a timestamp indicating when the record was inserted or updated. 
One thing which is problematic here is how deletions are handled. 
There could be an extra flag saying that a record was deleted (including the deletion time). 
Or it could be that deletions are not transferred at all, either because they don't happen or for other reasons.

## DBMerge

Both scenarios are supported with ETLBox.

The `DBMerge` component can be used to tackle this problem

The `DBMerge` is created on a destination table in your dataflow. 
It will wait for all data from the flow to arrive, and then either insert, 
update or delete the data in the detstination table. 
Deletion is optional (by default turned on) , and can be disabled with the property 
`DisableDeletion` set to true. 

The DBMergewas designed for scenario 1, but also  works for scenario 2 except deletions.

 To implement a sync between two tables you will  need a `DBSource` pointing to your source table. 
 In our case we just use a table name for the source, but you could also define a sql query 
 (e.g. which gives you only the delta records).

The source is then connected to the DBMerge, and data from the source will then be inserted, 
updated or deleted in the destination. 
The DBMerge  is a generic object and expect as type an object that implements the interface IMergeable. 
This interface needs to have a ChangeDate and ChangeAction defines on your object. 
It is used to store in the record if it needs to be inserted, updated or delete, and when this happened.

Optionally (but highly recommended) you should add the attribute `[MergeIdColumnName(...)]` to one of your 
properties of your object. It has two purposes: on the one hand, you pass a parameter the name of your database 
column which you would like to use to as identifier. This should be a unique column, e.g. a primary key. 
The property decorated with the attribute should return a unique value as well - if both match, 
the record is identified as a match. 

If a match occurs, the record does exist in the destination and the source. Now all column will be compared used
the `Equals(object other)` method - if they differ, the record will be marked to be updated. 
If they do not differ, they will be marked as already existing.
Records who exists in the source will be marked as instertions. 
Records missing in the source will be deleted from the destination (if `DisableDeletions = false`)

When the DBMerge is executed, it first will compare the source and destination and check what records 
needs to inserted, updated (if different), needs no change or needs to be deleted (if enabled). 
Then, it will delete all records from the source that are either missing or need to be updated. 
After this, it will insert the new and updated records into the destination. 

The DBMerge has a property `DeltaTable` which is a List containing additionally information what records 
where updated, existing,  inserted or deleted. The operation and change-date is stored in the corresponding 
`ChangeDate`/ `ChangeAction` properties.
This information can be used as a source for further processing in the data flow, 
simple by connecting the DBMerge to a transformation or another Destination.

Please note that if you connect the DBMerge to a source that provide you with delta information only, 
you need to disable the deletions - in that case, deletions need to be handled manually. 

## Example code 

```C#

public class MyMergeRow : IMergable
{
    [ColumnMap("Col1")]
    public long Key { get; set; }
    [ColumnMap("Col2")]
    public string Value { get; set; }

    /* IMergable interface */
    public DateTime ChangeDate { get; set; }
    public string ChangeAction { get; set; }

    [MergeIdColumnName("Col1")]
    public string UniqueId => Key.ToString();

    public override bool Equals(object other)
    {
        var msr = other as MyMergeRow;
        if (other == null) return false;
        return msr.Value == this.Value;
    }
}

[Theory, MemberData(nameof(Connections))]
public void SimpleMerge(IConnectionManager connection)
{
    //Arrange
    TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(connection, "DBMergeSource");
    s2c.InsertTestData();
    s2c.InsertTestDataSet2();
    TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(connection, "DBMergeDestination");
    d2c.InsertTestDataSet3();
    DBSource<MyMergeRow> source = new DBSource<MyMergeRow>(connection, "DBMergeSource");

    //Act
    DBMerge<MyMergeRow> dest = new DBMerge<MyMergeRow>(connection, "DBMergeDestination");
    source.LinkTo(dest);
    source.Execute();
    dest.Wait();

    //Assert
    Assert.Equal(6, RowCountTask.Count(connection, "DBMergeDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
    Assert.True(dest.DeltaTable.Count == 7);
    Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "U").Count() == 2);
    Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "D" && row.Key == 10).Count() == 1);
    Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "I").Count() == 3);
    Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == "E" && row.Key == 1).Count() == 1);
}
```