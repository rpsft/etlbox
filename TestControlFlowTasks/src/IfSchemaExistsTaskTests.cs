using ALE.ETLBox.src.Definitions.ConnectionManager;
using ALE.ETLBox.src.Toolbox.ConnectionManager.Native;
using ALE.ETLBox.src.Toolbox.ControlFlow.Database;
using TestControlFlowTasks.src.Fixtures;

namespace TestControlFlowTasks.src
{
    public class IfSchemaExistsTaskTests : ControlFlowTestBase
    {
        public IfSchemaExistsTaskTests(ControlFlowDatabaseFixture fixture)
            : base(fixture) { }

        public static IEnumerable<object[]> Connections => AllConnectionsWithoutSQLite;

        [Theory, MemberData(nameof(Connections))]
        public void IfSchemaExists(IConnectionManager connection)
        {
            if (connection.GetType() == typeof(MySqlConnectionManager))
            {
                return;
            }

            //Arrange
            var existsBefore = IfSchemaExistsTask.IsExisting(connection, "testschema");
            CreateSchemaTask.Create(connection, "testschema");

            //Act
            var existsAfter = IfSchemaExistsTask.IsExisting(connection, "testschema");

            //Assert
            Assert.False(existsBefore);
            Assert.True(existsAfter);
        }
    }
}
