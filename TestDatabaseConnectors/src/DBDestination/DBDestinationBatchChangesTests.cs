using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using ALE.ETLBox.src.Toolbox.DataFlow;
using TestDatabaseConnectors.src.Fixtures;
using TestShared.src.SharedFixtures;

namespace TestDatabaseConnectors.src.DBDestination
{
    public class DbDestinationBatchChangesTests : DatabaseConnectorsTestBase
    {
        public DbDestinationBatchChangesTests(DatabaseSourceDestinationFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void WithBatchChanges(IConnectionManager connection)
        {
            //Arrange
            var d2C = new TwoColumnsTableFixture(
                connection,
                "DbDestinationBatchChanges"
            );
            var dest = new DbDestination<string[]>(
                connection,
                "DbDestinationBatchChanges",
                batchSize: 2
            )
            {
                BeforeBatchWrite = rowArray =>
                {
                    rowArray[0][1] = "NewValue";
                    return rowArray;
                }
            };

            //Act
            var source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DbDestinationBatchChanges"));
            Assert.Equal(
                2,
                RowCountTask.Count(
                    connection,
                    "DbDestinationBatchChanges",
                    $"{d2C.QB}Col2{d2C.QE}='NewValue'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DbDestinationBatchChanges",
                    $"{d2C.QB}Col1{d2C.QE} = 2 AND {d2C.QB}Col2{d2C.QE}='Test2'"
                )
            );
        }

        [Theory, MemberData(nameof(AllSqlConnections))]
        public void AfterBatchWrite(IConnectionManager connection)
        {
            //Arrange
            var wasExecuted = false;
            var _ = new TwoColumnsTableFixture(connection, "DbDestinationBatchChanges");
            var dest = new DbDestination<string[]>(
                connection,
                "DbDestinationBatchChanges",
                batchSize: 1
            )
            {
                AfterBatchWrite = rowArray =>
                {
                    Assert.True(rowArray.Length == 1);
                    wasExecuted = true;
                }
            };

            //Act
            var source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DbDestinationBatchChanges"));
            Assert.True(wasExecuted);
        }
    }
}
