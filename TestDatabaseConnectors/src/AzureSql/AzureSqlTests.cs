using ETLBox.ConnectionManager;
using ETLBox.ControlFlow;
using ETLBox.ControlFlow.SqlServer;
using ETLBox.DataFlow;
using ETLBox.Helper;
using ETLBox.SqlServer;
using ETLBoxTests.Fixtures;
using System;
using System.Linq;
using Xunit;

namespace ETLBoxTests.DataFlowTests
{
    public sealed class IgnoreOnNonAzureEnvironmentFact : FactAttribute
    {
        public IgnoreOnNonAzureEnvironmentFact()
        {
            if (!IsInAzure())
                Skip = "Ignore on non azure environments";
        }

        private static bool IsInAzure()
            => Environment.GetEnvironmentVariable("ETLBoxAzure") != null;
    }

    [Collection("DataFlow")]
    public class AzureSqlTests
    {
        public static SqlConnectionManager AzureSqlConnection => Config.AzureSqlConnection.ConnectionManager("DataFlow");

        public static SqlConnectionManager SqlConnection => Config.SqlConnection.ConnectionManager("DataFlow");


        public AzureSqlTests(DataFlowDatabaseFixture dbFixture)
        {
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[source]");
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[dest]");
            CreateSchemaTask.Create(AzureSqlConnection, "[source]");
            CreateSchemaTask.Create(AzureSqlConnection, "[dest]");
        }

        public class MySimpleRow : MergeableRow
        {
            [IdColumn]
            public int Col1 { get; set; }
            [CompareColumn]
            public string Col2 { get; set; }
        }

        [IgnoreOnNonAzureEnvironmentFact]
        public void ReadAndWriteToAzure()
        {
            var envvar = Environment.GetEnvironmentVariable("ETLBoxAzure");
            if (envvar != "true") return;
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(AzureSqlConnection, "[source].[AzureSource]");
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(AzureSqlConnection, "[dest].[AzureDestination]");

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(AzureSqlConnection, "[source].[AzureSource]");
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(AzureSqlConnection, "[dest].[AzureDestination]");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            dest2Columns.AssertTestData();
        }

        [IgnoreOnNonAzureEnvironmentFact]
        public void MergeIntoAzure()
        {
            var envvar = Environment.GetEnvironmentVariable("ETLBoxAzure");
            if (envvar != "true") return;
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(AzureSqlConnection, "[dest].[AzureMergeDestination]");
            d2c.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(SqlConnection, "DBMergeSource");

            //Act
            DbMerge<MySimpleRow> dest = new DbMerge<MySimpleRow>(AzureSqlConnection, "[dest].[AzureMergeDestination]");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(6, RowCountTask.Count(AzureSqlConnection, "[dest].[AzureMergeDestination]", $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"));
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Update).Count() == 2);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Delete && row.Col1 == 10).Count() == 1);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Insert).Count() == 3);
            Assert.True(dest.DeltaTable.Where(row => row.ChangeAction == ChangeAction.Exists && row.Col1 == 1).Count() == 1);

        }


    }
}
