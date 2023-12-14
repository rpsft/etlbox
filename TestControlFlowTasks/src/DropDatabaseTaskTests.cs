using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Definitions.Exceptions;
using ALE.ETLBox.src.Helper;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class DropDatabaseTaskTests : ControlFlowTestBase
    {
        public DropDatabaseTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(DbConnectionsWithMaster))]
        public void Drop(IConnectionManager connection)
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            CreateDatabaseTask.Create(connection, dbName);
            var existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.Drop(connection, dbName);

            //Assert
            var existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Theory, MemberData(nameof(DbConnectionsWithMaster))]
        public void DropIfExists(IConnectionManager connection)
        {
            //Arrange
            var dbName = "ETLBox_" + HashHelper.RandomString(10);
            DropDatabaseTask.DropIfExists(connection, dbName);
            CreateDatabaseTask.Create(connection, dbName);
            var existsBefore = IfDatabaseExistsTask.IsExisting(connection, dbName);

            //Act
            DropDatabaseTask.DropIfExists(connection, dbName);

            //Assert
            var existsAfter = IfDatabaseExistsTask.IsExisting(connection, dbName);
            Assert.True(existsBefore);
            Assert.False(existsAfter);
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropDatabaseTask.Drop(SqliteConnection, "Test")
            );
        }
    }
}
