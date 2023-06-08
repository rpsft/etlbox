using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class IfDatabaseExistsTaskTests
    {
        public static IEnumerable<object[]> Connections =>
            Config.AllConnectionsWithoutSQLite("ControlFlow");

        [Theory, MemberData(nameof(Connections))]
        public void IfDatabaseExists(IConnectionManager connection)
        {
            //Arrange
            string dbName = ("ETLBox_" + HashHelper.RandomString(10)).ToLower();
            var existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            SqlTask.ExecuteNonQuery(connection, "Create DB", $"CREATE DATABASE {dbName}");
            var existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () =>
                    IfDatabaseExistsTask.IsExisting(
                        Config.SQLiteConnection.ConnectionManager("ControlFlow"),
                        "Test"
                    )
            );
        }
    }
}
