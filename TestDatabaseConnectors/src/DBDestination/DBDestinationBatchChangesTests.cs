using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using TestShared.Helper;
using TestShared.SharedFixtures;

namespace TestDatabaseConnectors.DBDestination
{
    [Collection("DataFlow")]
    public class DbDestinationBatchChangesTests
    {
        public static IEnumerable<object[]> Connections => Config.AllSqlConnections("DataFlow");

        [Theory, MemberData(nameof(Connections))]
        public void WithBatchChanges(IConnectionManager connection)
        {
            //Arrange
            TwoColumnsTableFixture d2c = new TwoColumnsTableFixture(
                connection,
                "DbDestinationBatchChanges"
            );
            DbDestination<string[]> dest = new DbDestination<string[]>(
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
            CsvSource<string[]> source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
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
                    $"{d2c.QB}Col2{d2c.QE}='NewValue'"
                )
            );
            Assert.Equal(
                1,
                RowCountTask.Count(
                    connection,
                    "DbDestinationBatchChanges",
                    $"{d2c.QB}Col1{d2c.QE} = 2 AND {d2c.QB}Col2{d2c.QE}='Test2'"
                )
            );
        }

        [Theory, MemberData(nameof(Connections))]
        public void AfterBatchWrite(IConnectionManager connection)
        {
            //Arrange
            bool wasExecuted = false;
            var _ = new TwoColumnsTableFixture(connection, "DbDestinationBatchChanges");
            DbDestination<string[]> dest = new DbDestination<string[]>(
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
            CsvSource<string[]> source = new CsvSource<string[]>("res/BatchChanges/TwoColumns.csv");
            source.LinkTo(dest);
            source.Execute();
            dest.Wait();

            //Assert
            Assert.Equal(3, RowCountTask.Count(connection, "DbDestinationBatchChanges"));
            Assert.True(wasExecuted);
        }
    }
}
