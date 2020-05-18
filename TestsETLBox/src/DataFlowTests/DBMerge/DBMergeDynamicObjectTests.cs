using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using ALE.ETLBox.Helper;
using ALE.ETLBox.Logging;
using ALE.ETLBoxTests.Fixtures;
using Newtonsoft.Json;
using Org.BouncyCastle.Asn1.Esf;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
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
            //Arrange
            MemorySource source = new MemorySource();
            source.DataAsList.Add(CreateDynamicRow(1, "Test1"));
            source.DataAsList.Add(CreateDynamicRow(2, "Test2"));
            source.DataAsList.Add(CreateDynamicRow(3, "Test3"));
            //source.DataAsList.Add(new MyMergeRow() { Key = 4, DeleteThisRow = true });
            //source.DataAsList.Add(new MyMergeRow() { Key = 10, DeleteThisRow = true });
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeDynamicDestination");
            d2c.InsertTestDataSet3();

            //Act
            DbMerge<ExpandoObject> dest = new DbMerge<ExpandoObject>(SqlConnection, "DBMergeDynamicDestination");
            dest.PropNames.IdColumns.Add("Col1");
            dest.PropNames.CompareColumns.Add("Col2");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(SqlConnection, "DBMergeDynamicDestination", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            //Assert.True(dest.DeltaTable.Count == 7);
            //Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update).Count() == 2);
            //Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Delete && row.Key == 10).Count() == 1);
            //Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Insert).Count() == 3);
            //Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Key == 1).Count() == 1);
        }

        private dynamic CreateDynamicRow(int key, string value)
        {
            dynamic r = new ExpandoObject();
            r.Col1 = key;
            r.Col2 = value;
            r.AreEqual = new AreEqual( (a,b) =>
            {
                dynamic c = a as ExpandoObject;
                dynamic d = b as ExpandoObject;
                return c.Col2 == d.Col2;
            });
            return r;
        }
        public delegate bool AreEqual(ExpandoObject a, ExpandoObject b);
    }
}
