using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;

namespace TestControlFlowTasks
{
    [Collection("ControlFlow")]
    public class CreateDatabaseTaskTests
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
        public void CreateSimple(IConnectionManager connection)
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            var dbListBefore = GetDatabaseListTask.List(connection);
            Assert.DoesNotContain(dbName, dbListBefore);

            //Act
            CreateDatabaseTask.Create(connection, dbName);

            //Assert
            var dbListAfter = GetDatabaseListTask.List(connection);
            Assert.Contains(dbName, dbListAfter);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        [Theory, MemberData(nameof(SqlConnectionsWithMaster))]
        public void CreateWithCollation(IConnectionManager connection)
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            var collation = "Latin1_General_CS_AS";
            if (connection.GetType() == typeof(PostgresConnectionManager))
                collation = "en_US.utf8";
            if (connection.GetType() == typeof(MySqlConnectionManager))
                collation = "latin1_swedish_ci";
            //Act
            CreateDatabaseTask.Create(connection, dbName, collation);

            //Assert
            var dbList = GetDatabaseListTask.List(connection);
            Assert.Contains(dbName, dbList);

            //Cleanup
            DropDatabaseTask.Drop(connection, dbName);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () =>
                    CreateDatabaseTask.Create(
                        Config.SQLiteConnection.ConnectionManager("ControlFlow"),
                        "Test"
                    )
            );
        }
    }
}
