using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class DropDatabaseTaskTests
    {
        public static IEnumerable<object[]> SqlConnectionsWithMaster() =>
            new[]
            {
                new object[]
                {
                    new SqlConnectionManager(
                        Config.SqlConnection.ConnectionString("ControlFlow").CloneWithMasterDbName()
                    )
                },
                new object[]
                {
                    new PostgresConnectionManager(
                        Config.PostgresConnection
                            .ConnectionString("ControlFlow")
                            .CloneWithMasterDbName()
                    )
                },
                new object[]
                {
                    new MySqlConnectionManager(
                        Config.MySqlConnection
                            .ConnectionString("ControlFlow")
                            .CloneWithMasterDbName()
                    )
                },
            };

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_" + HashHelper.RandomString(10);
            CreateDatabaseTask.Create(connection, dbName);
            bool existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.Drop(connection, dbName);

            //Assert
            bool existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            string dbName = "ETLBox_" + HashHelper.RandomString(10);
            DropDatabaseTask.DropIfExists(connection, dbName);
            CreateDatabaseTask.Create(connection, dbName);
            bool existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.DropIfExists(connection, dbName);

            //Assert
            bool existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () =>
                    DropDatabaseTask.Drop(
                        Config.SQLiteConnection.ConnectionManager("ControlFlow"),
                        "Test"
                    )
            );
        }
    }
}
