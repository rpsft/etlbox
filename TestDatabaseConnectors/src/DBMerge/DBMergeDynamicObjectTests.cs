using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBoxTests.Fixtures;
using System.Dynamic;
using Xunit;

namespace ALE.ETLBoxTests.DataFlowTests
{
    [Collection("DataFlow")]
    public class DbMergeDynamicObjectTests
    {
        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");

        public DbMergeDynamicObjectTests(DataFlowDatabaseFixture dbFixture)
        {
        }

        [Fact]
        public void SimpleMergeWithDynamic()
        {
            //Arrange
            MemorySource source = new MemorySource();
            source.DataAsList.Add(CreateDynamicRow(1, "Test1"));
            source.DataAsList.Add(CreateDynamicRow(2, "Test2"));
            source.DataAsList.Add(CreateDynamicRow(3, "Test3"));
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeDynamicDestination");
            d2c.InsertTestDataSet3();

            //Act
            DbMerge dest = new DbMerge(SqlConnection, "DBMergeDynamicDestination");
            dest.MergeProperties.IdPropertyNames.Add("Col1");
            dest.MergeProperties.ComparePropertyNames.Add("Col2");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "DBMergeDynamicDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            d2c.AssertTestData();
            Assert.Collection<ExpandoObject>(dest.DeltaTable,
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Exists && r.Col1 == 1); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Update && r.Col1 == 2); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Insert && r.Col1 == 3); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Delete && r.Col1 == 4); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Delete && r.Col1 == 10); }
                );
        }

        private dynamic CreateDynamicRow(int key, string value = "", bool delete = false)
        {
            dynamic r = new ExpandoObject();
            r.Col1 = key;
            if (!string.IsNullOrWhiteSpace(value))
                r.Col2 = value;
            if (delete)
                r.Delete = true;
            return r;
        }

        [Fact]
        public void DeltaLoadWithDeletion()
        {
            //Arrange
            MemorySource source = new MemorySource();
            source.DataAsList.Add(CreateDynamicRow(2, "Test2"));
            source.DataAsList.Add(CreateDynamicRow(3, "Test3"));
            source.DataAsList.Add(CreateDynamicRow(4, delete: true));
            source.DataAsList.Add(CreateDynamicRow(10, delete: true));
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeDynamicDeltaDestination");
            d2c.InsertTestDataSet3();

            //Act
            DbMerge dest = new DbMerge(SqlConnection, "DBMergeDynamicDeltaDestination")
            {
                DeltaMode = DeltaMode.Delta
            };
            dest.MergeProperties.IdPropertyNames.Add("Col1");
            dest.MergeProperties.ComparePropertyNames.Add("Col2");
            dest.MergeProperties.DeletionProperties.Add("Delete", true);
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            d2c.AssertTestData();
            Assert.Collection<ExpandoObject>(dest.DeltaTable,
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Update && r.Col1 == 2); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Insert && r.Col1 == 3); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Delete && r.Col1 == 4); },
                row => { dynamic r = row as ExpandoObject; Assert.True(r.ChangeAction == ChangeAction.Delete && r.Col1 == 10); }
            );
        }
    }
}
