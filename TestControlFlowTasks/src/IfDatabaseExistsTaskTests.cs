using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Helper;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class IfDatabaseExistsTaskTests : ControlFlowTestBase
    {
        public IfDatabaseExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

        [Theory, MemberData(nameof(Connections))]
        public void IfDatabaseExists(IConnectionManager connection)
        {
            //Arrange
            var dbName = ("ETLBox_" + HashHelper.RandomString(10)).ToLower();
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
                () => IfDatabaseExistsTask.IsExisting(SqliteConnection, "Test")
            );
        }
    }
}
