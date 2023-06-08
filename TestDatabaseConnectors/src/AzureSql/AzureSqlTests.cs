using System;
using System.Linq;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.AzureSql
{
    public sealed class IgnoreOnNonAzureEnvironmentFact : FactAttribute
    {
        public IgnoreOnNonAzureEnvironmentFact()
        {
            if (!IsInAzure())
                Skip = "Ignore on non azure environments";
        }

        private static bool IsInAzure() =>
            Environment.GetEnvironmentVariable("ETLBoxAzure") != null;
    }

    [Collection("DataFlow")]
    public class AzureSqlTests
    {
        private static SqlConnectionManager AzureSqlConnection =>
            Config.AzureSqlConnection.ConnectionManager("DataFlow");

        private static SqlConnectionManager SqlConnection =>
            Config.SqlConnection.ConnectionManager("DataFlow");

        public AzureSqlTests()
        {
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[source]");
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[dest]");
            CreateSchemaTask.Create(AzureSqlConnection, "[source]");
            CreateSchemaTask.Create(AzureSqlConnection, "[dest]");
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
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
            if (envvar != "true")
                return;
            //Arrange
            TwoColumnsTableFixture source2Columns = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[source].[AzureSource]"
            );
            source2Columns.InsertTestData();
            TwoColumnsTableFixture dest2Columns = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[dest].[AzureDestination]"
            );

            //Act
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                AzureSqlConnection,
                "[source].[AzureSource]"
            );
            DbDestination<MySimpleRow> dest = new DbDestination<MySimpleRow>(
                AzureSqlConnection,
                "[dest].[AzureDestination]"
            );
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
            if (envvar != "true")
                return;
            //Arrange
            TwoColumnsTableFixture s2c = new TwoColumnsTableFixture(SqlConnection, "DBMergeSource");
            s2c.InsertTestData();
            s2c.InsertTestDataSet2();
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[dest].[AzureMergeDestination]"
            );
            d2c.InsertTestDataSet3();
            DbSource<MySimpleRow> source = new DbSource<MySimpleRow>(
                SqlConnection,
                "DBMergeSource"
            );

            //Act
            DbMerge<MySimpleRow> dest = new DbMerge<MySimpleRow>(
                AzureSqlConnection,
                "[dest].[AzureMergeDestination]"
            );
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(
                6,
                RowCountTask.Count(
                    AzureSqlConnection,
                    "[dest].[AzureMergeDestination]",
                    $"{d2c.QB}Col1{d2c.QE} BETWEEN 1 AND 7 AND {d2c.QB}Col2{d2c.QE} LIKE 'Test%'"
                )
            );
            Assert.True(dest.DeltaTable.Count == 7);
            Assert.True(dest.DeltaTable.Count(row => row.ChangeAction == ChangeAction.Update) == 2);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Delete && row.Col1 == 10
                ) == 1
            );
            Assert.True(dest.DeltaTable.Count(row => row.ChangeAction == ChangeAction.Insert) == 3);
            Assert.True(
                dest.DeltaTable.Count(
                    row => row.ChangeAction == ChangeAction.Exists && row.Col1 == 1
                ) == 1
            );
        }
    }
}
