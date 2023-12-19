using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.ControlFlow.SqlServer;
using ALE.ETLBox.DataFlow;
using TestDatabaseConnectors.Fixtures;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.AzureSql
{
    public sealed class IgnoreOnNonAzureEnvironmentFactAttribute : FactAttribute
    {
        public IgnoreOnNonAzureEnvironmentFactAttribute()
        {
            if (!IsInAzure())
                Skip = "Ignore on non azure environments";
        }

        private static bool IsInAzure() =>
            Environment.GetEnvironmentVariable("ETLBoxAzure") != null;
    }

    public sealed class AzureSqlTests : DatabaseConnectorsTestBase, IDisposable
    {
        public AzureSqlTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture)
        {
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[source]");
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[dest]");
            CreateSchemaTask.Create(AzureSqlConnection, "[source]");
            CreateSchemaTask.Create(AzureSqlConnection, "[dest]");
        }

        public void Dispose()
        {
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[source]");
            CleanUpSchemaTask.CleanUp(AzureSqlConnection, "[dest]");
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
            var source2Columns = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[source].[AzureSource]"
            );
            source2Columns.InsertTestData();
            var dest2Columns = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[dest].[AzureDestination]"
            );

            //Act
            var source = new DbSource<MySimpleRow>(
                AzureSqlConnection,
                "[source].[AzureSource]"
            );
            var dest = new DbDestination<MySimpleRow>(
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
            var s2C = new TwoColumnsTableFixture(SqlConnection, "DBMergeSource");
            s2C.InsertTestData();
            s2C.InsertTestDataSet2();
            var d2C = new TwoColumnsTableFixture(
                AzureSqlConnection,
                "[dest].[AzureMergeDestination]"
            );
            d2C.InsertTestDataSet3();
            var source = new DbSource<MySimpleRow>(
                SqlConnection,
                "DBMergeSource"
            );

            //Act
            var dest = new DbMerge<MySimpleRow>(
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
                    $"{d2C.QB}Col1{d2C.QE} BETWEEN 1 AND 7 AND {d2C.QB}Col2{d2C.QE} LIKE 'Test%'"
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
