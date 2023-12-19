using ALE.ETLBox;
using ALE.ETLBox.Common;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class DropDatabaseTaskTests : ControlFlowTestBase
    {
        public DropDatabaseTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        [Theory, MemberData(nameof(DbConnectionsWithMaster))]
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

        [Theory, MemberData(nameof(DbConnectionsWithMaster))]
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
                () => DropDatabaseTask.Drop(SqliteConnection, "Test")
            );
        }
    }
}
