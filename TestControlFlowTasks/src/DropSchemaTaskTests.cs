using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.Primitives;
using TestControlFlowTasks.Fixtures;

namespace TestControlFlowTasks
{
    public class DropSchemaTaskTests : ControlFlowTestBase
    {
        public DropSchemaTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

        [Theory, MemberData(nameof(Connections))]
        public void Drop(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            CreateSchemaTask.Create(connection, "testcreateschema");
            Assert.True(IfSchemaExistsTask.IsExisting(connection, "testcreateschema"));

            //Act
            DropSchemaTask.Drop(connection, "testcreateschema");

            //Assert
            Assert.False(IfSchemaExistsTask.IsExisting(connection, "testcreateschema"));
        }

        [Theory, MemberData(nameof(Connections))]
        public void DropIfExists(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            DropSchemaTask.DropIfExists(connection, "testcreateschema2");
            CreateSchemaTask.Create(connection, "testcreateschema2");
            Assert.True(IfSchemaExistsTask.IsExisting(connection, "testcreateschema2"));

            //Act
            DropSchemaTask.DropIfExists(connection, "testcreateschema2");

            //Assert
            Assert.False(IfSchemaExistsTask.IsExisting(connection, "testcreateschema2"));
        }

        [Fact]
        public void NotSupportedWithSQLite()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropSchemaTask.Drop(SqliteConnection, "Test")
            );
        }

        [Fact]
        public void NotSupportedWithMySql()
        {
            Assert.Throws<ETLBoxNotSupportedException>(
                () => DropSchemaTask.Drop(SqliteConnection, "Test")
            );
        }
    }
}
